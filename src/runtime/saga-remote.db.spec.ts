import { randomUUID } from 'node:crypto';

import { OUTBOX_ORDERING } from '@hoshinotatsuya/outbox';
import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaStateError } from '../errors/saga-state.error';
import { sagaReplyMessage } from '../remote/saga-reply-message';
import { sagaReplyOf } from '../remote/saga-reply-of';
import {
  type BillingScript,
  billingParticipant,
  newBillingScript,
  paymentSaga,
} from '../testing/billing-participant';
import { startAsV010 } from '../testing/legacy-v010';
import {
  createReplica,
  effectsOf,
  instanceOf,
  journalOf,
  pump,
  recordEffect,
  type SagaReplica,
  START,
  type TestClock,
} from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import { sagaMessageId } from './saga-message-id';

/** Удалённый шаг: команда через outbox, ответ через inbox, таймер ответа через deliverAt. */
describeDb('SagaOrchestrator: удалённые шаги', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let billing: BillingScript;
  let replica: SagaReplica;

  const definition = paymentSaga();
  const lastCommand = (phase: 'execute' | 'compensate'): SagaCommandMeta => {
    const found = billing.received.filter((meta) => meta.phase === phase).at(-1);
    if (found === undefined) {
      throw new Error(`участник не получал команд фазы ${phase}`);
    }
    return found;
  };

  /** Проходы релея без сдвига часов: шаг reserve → отправка команды → доставка участнику. */
  const passes = async (count: number): Promise<void> => {
    for (let pass = 0; pass < count; pass += 1) {
      await replica.relay.runOnce();
    }
  };

  beforeAll(async () => {
    database = await createTestDatabase();
    await createSagaSchema(database.pool);
  });

  afterAll(async () => {
    await database?.drop();
  });

  beforeEach(async () => {
    await truncateSagaSchema(database.pool);
    clock = { now: new Date(START) };
    billing = newBillingScript();
    replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
  });

  it('счастливый путь: команда участнику, ответ вливает данные в контекст, следующий шаг их видит', async () => {
    await replica.start(definition, 'p-1', { orderId: '1' });
    await pump(clock, [replica]);

    const instance = await instanceOf(replica, 'p-1');
    expect({ status: instance.status, context: instance.context }).toEqual({
      status: SAGA_STATUS.completed,
      context: { orderId: '1', reservationId: 'r-1', chargeId: 'ch-p-1', shipped: 'ch-p-1' },
    });
    expect(await effectsOf(database.pool, 'p-1')).toEqual([
      'reserve:execute',
      'billing:execute',
      'ship:execute',
    ]);
    expect(await journalOf(replica, 'p-1')).toEqual([
      'saga-started',
      'step-completed:reserve',
      'step-dispatched:charge',
      'step-completed:charge',
      'step-completed:ship',
      'saga-completed',
    ]);
    const created = (await instanceOf(replica, 'p-1')).incarnation;
    expect(billing.received.map((meta) => [meta.step, meta.stepKey, meta.attempt, meta.incarnation])).toEqual(
      [['charge', `p-1#${created}:execute:1`, 1, created]],
    );
    // Таймер ответа отработал после ответа и ничего не сдвинул.
    const { rows } = await database.pool.query<{ status: string }>(
      `SELECT status FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.timer'`,
    );
    expect(rows).toEqual([{ status: 'published' }]);
  });

  it('outbox с FIFO по агрегату: таймер ответа не держит ни команду участнику, ни ответ, ни следующий шаг', async () => {
    // Прежде таймер лежал в агрегате экземпляра с deliverAt = +timeoutMs:
    // FIFO агрегата держал за ним ответ и следующую команду (а при меньшем
    // uuid — и саму команду участнику) до срока таймера, и шаг ложно
    // отваливался по таймауту.
    await replica.relay.stop({ deadlineMs: 1_000 });
    replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
      ordering: OUTBOX_ORDERING.aggregate,
    });
    await replica.start(definition, 'p-fifo', { orderId: '1' });
    await pump(clock, [replica]);

    expect({
      status: (await instanceOf(replica, 'p-fifo')).status,
      journal: await journalOf(replica, 'p-fifo'),
    }).toEqual({
      status: SAGA_STATUS.completed,
      journal: [
        'saga-started',
        'step-completed:reserve',
        'step-dispatched:charge',
        'step-completed:charge',
        'step-completed:ship',
        'saga-completed',
      ],
    });
    expect(billing.received.map((meta) => meta.attempt)).toEqual([1]);
  });

  it('FIFO по агрегату: поздний успех прежней попытки применяется сразу, а не после отложенного повтора', async () => {
    await replica.relay.stop({ deadlineMs: 1_000 });
    replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
      ordering: OUTBOX_ORDERING.aggregate,
    });
    billing.execute.push('silent');
    await replica.start(definition, 'p-late-fifo', { orderId: '1' });
    await passes(6);
    const first = lastCommand('execute');
    // Таймер попытки 1 созрел: повтор поставлен с паузой (deliverAt в будущем).
    clock.now = new Date(clock.now.getTime() + 30_001);
    await passes(3);
    expect((await instanceOf(replica, 'p-late-fifo')).attempt).toBe(2);

    // Ответ записан ПОЗЖЕ отложенного повтора (в ту же миллисекунду порядок
    // решал бы uuid — тест проверял бы случай, а не FIFO).
    clock.now = new Date(clock.now.getTime() + 1);
    await replica.transaction(async (tx) => {
      await replica.outbox.enqueue(
        tx,
        [sagaReplyMessage(sagaReplyOf(first, { ok: true, data: { chargeId: 'поздний' } }))],
        clock.now,
      );
    });
    await pump(clock, [replica]);

    const instance = await instanceOf(replica, 'p-late-fifo');
    expect({
      status: instance.status,
      chargeId: instance.context.chargeId,
      commands: billing.received.filter((meta) => meta.phase === 'execute').length,
    }).toEqual({ status: SAGA_STATUS.completed, chargeId: 'поздний', commands: 1 });
  });

  it('prune до срока таймера ответа: таймер удалённого экземпляра — устаревший, а не повторы и DEAD', async () => {
    await replica.start(definition, 'p-pruned', { orderId: '1' });
    // До тишины БЕЗ сдвига часов: сага доезжает, таймер ответа ждёт своего срока.
    for (let round = 0; round < 50; round += 1) {
      if ((await replica.relay.runOnce()).claimed === 0) {
        break;
      }
    }
    expect((await instanceOf(replica, 'p-pruned')).status).toBe(SAGA_STATUS.completed);
    const pending = await database.pool.query<{ status: string }>(
      `SELECT status FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.timer'`,
    );
    expect(pending.rows).toEqual([{ status: 'pending' }]);

    expect(await replica.admin.prune(new Date(clock.now.getTime() + 1))).toBe(1);
    await pump(clock, [replica]);

    // Прежде: SagaNotFoundError на каждой доставке, повторы и DEAD.
    const { rows } = await database.pool.query<{ status: string }>(
      `SELECT status FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.timer'`,
    );
    expect(rows).toEqual([{ status: 'published' }]);
    // Ответ удалённому экземпляру — тоже устаревший, а не отказ доставки.
    const sent = billing.received[0];
    if (sent === undefined) {
      throw new Error('участник не получил команду');
    }
    expect(
      await replica.orchestrator.handleReply(sagaReplyOf({ ...sent, commandId: randomUUID() }, { ok: true })),
    ).toBe(SAGA_HANDLED.stale);
  });

  it('команда участнику не пишется в outbox (BigInt) — отказ шага без повторов и компенсации, а не DEAD и вечный running', async () => {
    const broken = defineSaga<{ orderId: string }>('payment-bigint', { version: 1 })
      .step('reserve', {
        execute: async (args) => {
          await recordEffect(args.tx, args.sagaId, 'reserve', 'execute', args.attempt);
        },
        compensate: (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'compensate', args.attempt),
      })
      .remoteStep('charge', {
        command: () => ({ eventType: 'billing.charge', payload: { amount: 10n } }),
        timeoutMs: 30_000,
      })
      .build();
    await replica.relay.stop({ deadlineMs: 1_000 });
    replica = createReplica({ pool: database.pool, clock, definitions: [broken] });
    await replica.start(broken, 'p-bigint', { orderId: '1' });
    await pump(clock, [replica]);

    expect({
      status: (await instanceOf(replica, 'p-bigint')).status,
      journal: await journalOf(replica, 'p-bigint'),
      effects: await effectsOf(database.pool, 'p-bigint'),
    }).toEqual({
      status: SAGA_STATUS.compensated,
      journal: [
        'saga-started',
        'step-completed:reserve',
        'step-failed:charge',
        'step-compensated:reserve',
        'saga-compensated',
      ],
      effects: ['reserve:execute', 'reserve:compensate'],
    });
  });

  it('ответ участника с данными, которых не сохранить (NUL), — отказ с неизвестным исходом: компенсируется и сам шаг', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-nul', { orderId: '1' });
    await passes(3);
    const command = lastCommand('execute');

    expect(
      await replica.orchestrator.handleReply(
        sagaReplyOf(command, { ok: true, data: { chargeId: 'a\u0000b' } }),
      ),
    ).toBe(SAGA_HANDLED.failureRecorded);
    await pump(clock, [replica]);

    expect({
      status: (await instanceOf(replica, 'p-nul')).status,
      refunds: billing.received.filter((meta) => meta.phase === 'compensate').length,
    }).toEqual({ status: SAGA_STATUS.compensated, refunds: 1 });
  });

  it('тот же id после prune: участник получает НОВЫЙ ключ шага и выполняет шаг заново, а не возвращает исход прежней саги', async () => {
    await replica.start(definition, 'p-reuse', { orderId: '1' });
    await pump(clock, [replica]);
    clock.now = new Date(clock.now.getTime() + 60 * 60_000);
    expect(await replica.admin.prune(clock.now)).toBe(1);
    clock.now = new Date(clock.now.getTime() + 1);
    await replica.start(definition, 'p-reuse', { orderId: '2' });
    await pump(clock, [replica]);

    const keys = billing.received.filter((meta) => meta.phase === 'execute').map((meta) => meta.stepKey);
    expect({
      status: (await instanceOf(replica, 'p-reuse')).status,
      distinctKeys: new Set(keys).size,
      billingEffects: (await effectsOf(database.pool, 'p-reuse')).filter((e) => e === 'billing:execute')
        .length,
      // Эффектов участника два — по одному на воплощение (таблица эффектов теста общая).
    }).toEqual({ status: SAGA_STATUS.completed, distinctKeys: 2, billingEffects: 2 });
  });

  it('экземпляр прежней версии пакета (steps — массив имён) доживает с прежним форматом ключа шага', async () => {
    await startAsV010(replica, definition, 'p-legacy', { orderId: '1' });
    await pump(clock, [replica]);

    const sent = billing.received.find((meta) => meta.sagaId === 'p-legacy');
    const instance = await instanceOf(replica, 'p-legacy');
    expect({
      key: sent?.stepKey,
      sent: sent?.incarnation,
      status: instance.status,
      incarnation: instance.incarnation,
    }).toEqual({
      key: 'p-legacy:execute:1',
      sent: undefined,
      incarnation: null,
      status: SAGA_STATUS.completed,
    });
  });

  it('поздний успех прежней попытки у ЗАВЕРШЁННОЙ саги — stale без записи late-reply', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-done-late', { orderId: '1' });
    await passes(6);
    const first = lastCommand('execute');
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'p-done-late')).status).toBe(SAGA_STATUS.completed);

    expect(await replica.orchestrator.handleReply(sagaReplyOf(first, { ok: true }))).toBe(SAGA_HANDLED.stale);
    expect((await journalOf(replica, 'p-done-late')).at(-1)).toBe('saga-completed');
  });

  it('успех КОМПЕНСАЦИИ текущего поколения у идущей саги (подделка или ошибка участника) — stale без late-reply', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-forged', { orderId: '1' });
    await passes(6);
    const charge = lastCommand('execute');
    const journal = await journalOf(replica, 'p-forged');

    // id — как выдал бы оркестратор для этого адреса: иначе ответ отсеяла бы
    // сверка воплощения, и проверка «компенсации не было» ничего бы не доказала.
    const forged = sagaReplyOf(
      {
        ...charge,
        phase: 'compensate',
        commandId: sagaMessageId.remote({
          ...charge,
          phase: 'compensate',
          incarnation: (await instanceOf(replica, 'p-forged')).incarnation,
        }),
      },
      { ok: true },
    );
    expect(await replica.orchestrator.handleReply(forged)).toBe(SAGA_HANDLED.stale);
    expect({
      status: (await instanceOf(replica, 'p-forged')).status,
      journal: await journalOf(replica, 'p-forged'),
    }).toEqual({ status: SAGA_STATUS.running, journal });
  });

  it('ответ поколения ДО ручного повтора замёрзшего — stale и запись late-reply с его поколением', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-gen-late', { orderId: '1' });
    await passes(6);
    const first = lastCommand('execute');
    clock.now = new Date(clock.now.getTime() + 5 * 60_000);
    await replica.admin.retry('p-gen-late', 'замёрзла', { stalledForMs: 4 * 60_000 });

    expect(await replica.orchestrator.handleReply(sagaReplyOf(first, { ok: true }))).toBe(SAGA_HANDLED.stale);
    const shown = await replica.admin.show('p-gen-late');
    expect(shown?.journal.at(-1)).toEqual(
      expect.objectContaining({ event: 'late-reply', generation: 0, phase: 'execute' }),
    );
  });

  it('stalledReport: ждущий таймера ответа — не замёрзший до срока таймера, после срока — замёрзший', async () => {
    // Прежде срез смотрел только на updated_at, а отправка команды строку не
    // трогала: сага, штатно ждущая ответа (timeoutMs 30 с), через 20 с при
    // пороге 10 с числилась замёрзшей и могла быть «повторена» оператором.
    billing.execute.push('silent');
    await replica.start(definition, 'p-wait', { orderId: '1' });
    await passes(6);
    const dispatchedAt = clock.now.getTime();

    clock.now = new Date(dispatchedAt + 20_000);
    expect(await replica.admin.stalledReport(10_000)).toEqual([]);
    await expect(replica.admin.retry('p-wait', null, { stalledForMs: 10_000 })).rejects.toThrow(
      SagaStateError,
    );

    clock.now = new Date(dispatchedAt + 45_000);
    expect((await replica.admin.stalledReport(10_000)).map((item) => item.id)).toEqual(['p-wait']);
  });

  it('cancel идущей саги: новое поколение, возврат по ушедшей команде, компенсации; поздний ответ — stale', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-cancel', { orderId: '1' });
    await passes(6);
    const charge = lastCommand('execute');
    const generation = (await instanceOf(replica, 'p-cancel')).generation;

    const result = await replica.transaction((tx) =>
      replica.orchestrator.cancel(tx, 'p-cancel', 'клиент отменил заказ'),
    );
    // Новое поколение — граница для сообщений до отмены (команда, таймер,
    // ответ), не зависящая от фазы: компенсации идут с ним, а не с прежним.
    expect((await instanceOf(replica, 'p-cancel')).generation).toBe(generation + 1);
    expect(result).toEqual({ cancelled: true, status: SAGA_STATUS.compensating });
    await pump(clock, [replica]);
    expect(lastCommand('compensate').generation).toBe(generation + 1);
    expect(await replica.orchestrator.handleReply(sagaReplyOf(charge, { ok: true }))).toBe(
      SAGA_HANDLED.stale,
    );

    const instance = await instanceOf(replica, 'p-cancel');
    expect({
      status: instance.status,
      failure: instance.failure,
      refunds: billing.received.filter((meta) => meta.phase === 'compensate').length,
      journal: (await journalOf(replica, 'p-cancel')).filter(
        (e) => e.startsWith('saga-') || e.startsWith('step-compensated'),
      ),
    }).toEqual({
      status: SAGA_STATUS.compensated,
      failure: 'отменено: клиент отменил заказ',
      refunds: 1,
      journal: [
        'saga-started',
        'saga-cancelled',
        'step-compensated:charge',
        'step-compensated:reserve',
        'saga-compensated',
      ],
    });
    const done = await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'p-cancel', 'ещё раз'));
    expect(done).toEqual({ cancelled: false, status: SAGA_STATUS.compensated });
  });

  it('дубль ответа: два одновременных применения одного ответа — один applied, один duplicate; шаг сдвинут один раз', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-d', { orderId: 'd' });
    await passes(3);
    const reply = sagaReplyOf(lastCommand('execute'), { ok: true, data: { chargeId: 'ch-d' } });

    const results = await Promise.all([
      replica.orchestrator.handleReply(reply),
      replica.orchestrator.handleReply(reply),
    ]);
    const third = await replica.orchestrator.handleReply(reply);

    expect(results.sort()).toEqual([SAGA_HANDLED.applied, SAGA_HANDLED.duplicate].sort());
    expect(third).toBe(SAGA_HANDLED.duplicate);
    expect(
      (await journalOf(replica, 'p-d')).filter((entry) => entry === 'step-completed:charge'),
    ).toHaveLength(1);
    const { rows } = await database.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox}
        WHERE event_type = 'saga.command' AND payload->>'stepIndex' = '2'`,
    );
    expect(rows[0]?.n).toBe(1);
  });

  it('таймер: ответа нет — повтор шага; ответ на прежнюю попытку с отказом уже ничего не меняет', async () => {
    billing.execute.push('silent');
    await replica.start(definition, 'p-t', { orderId: 't' });
    await passes(3);
    const first = lastCommand('execute');
    await pump(clock, [replica]);

    expect((await instanceOf(replica, 'p-t')).status).toBe(SAGA_STATUS.completed);
    expect(billing.received.map((meta) => meta.attempt)).toEqual([1, 2]);
    expect((await journalOf(replica, 'p-t')).filter((entry) => entry.endsWith(':charge'))).toEqual([
      'step-dispatched:charge',
      'step-timed-out:charge',
      'step-dispatched:charge',
      'step-completed:charge',
    ]);
    const stale = await replica.orchestrator.handleReply(sagaReplyOf(first, { ok: false, error: 'поздно' }));
    expect(stale).toBe(SAGA_HANDLED.stale);
    expect((await instanceOf(replica, 'p-t')).status).toBe(SAGA_STATUS.completed);
  });

  it.each([
    ['успех прежней попытки принимается', true, SAGA_HANDLED.applied, 'ship'],
    ['отказ прежней попытки — stale', false, SAGA_HANDLED.stale, 'charge'],
  ] as const)('ответ на прежнюю попытку, пока ждут следующую: %s', async (_label, ok, handled, step) => {
    billing.execute.push('silent', 'silent');
    await replica.start(definition, 'p-l', { orderId: 'l' });
    await passes(3);
    const first = lastCommand('execute');
    // Таймер первой попытки: часы к его сроку — повтор; к сроку паузы — команда и доставка второй.
    clock.now = new Date(clock.now.getTime() + 30_000);
    await passes(1);
    clock.now = new Date(clock.now.getTime() + 1_000);
    await passes(2);
    expect(billing.received.map((meta) => meta.attempt)).toEqual([1, 2]);

    const result = await replica.orchestrator.handleReply(
      ok
        ? sagaReplyOf(first, { ok: true, data: { chargeId: 'ch-late' } })
        : sagaReplyOf(first, { ok: false, error: 'старый' }),
    );

    const instance = await instanceOf(replica, 'p-l');
    expect({ result, step: instance.steps[instance.stepIndex], attempt: instance.attempt }).toEqual({
      result: handled,
      step,
      attempt: ok ? 1 : 2,
    });
  });

  it('таймауты исчерпали повторы: исход неизвестен — компенсируется и сам шаг, затем завершённые, по порядку', async () => {
    billing.execute.push('silent', 'silent');
    await replica.start(definition, 'p-u', { orderId: 'u' });
    await pump(clock, [replica]);

    const instance = await instanceOf(replica, 'p-u');
    expect({ status: instance.status, failure: instance.failure }).toEqual({
      status: SAGA_STATUS.compensated,
      failure: 'нет ответа участника за 30000 мс',
    });
    expect(await effectsOf(database.pool, 'p-u')).toEqual([
      'reserve:execute',
      'billing:compensate',
      'reserve:compensate',
    ]);
    const created = (await instanceOf(replica, 'p-u')).incarnation;
    expect(lastCommand('compensate')).toEqual(
      expect.objectContaining({
        step: 'charge',
        stepKey: `p-u#${created}:compensate:1`,
        executeStepKey: `p-u#${created}:execute:1`,
      }),
    );
  });

  it('окончательный отказ участника: шаг НЕ выполнен — компенсируются только завершённые до него', async () => {
    billing.execute.push('reject');
    await replica.start(definition, 'p-r', { orderId: 'r' });
    await pump(clock, [replica]);

    expect((await instanceOf(replica, 'p-r')).status).toBe(SAGA_STATUS.compensated);
    expect(await effectsOf(database.pool, 'p-r')).toEqual(['reserve:execute', 'reserve:compensate']);
    expect(billing.received.map((meta) => meta.phase)).toEqual(['execute']);
  });

  it('отказ с повтором: вторая попытка участника проходит', async () => {
    billing.execute.push('fail');
    await replica.start(definition, 'p-f', { orderId: 'f' });
    await pump(clock, [replica]);

    expect((await instanceOf(replica, 'p-f')).status).toBe(SAGA_STATUS.completed);
    expect(billing.received.map((meta) => meta.attempt)).toEqual([1, 2]);
  });

  it('таймер ПРЕЖНЕЙ попытки (её отказ уже пришёл ответом) — stale; таймер текущей — отказ по таймауту', async () => {
    billing.execute.push('fail', 'silent');
    await replica.start(definition, 'p-o', { orderId: 'o' });
    // reserve → отправка попытки 1 → отказ участника → ответ: повтор через паузу.
    await passes(4);
    clock.now = new Date(clock.now.getTime() + 1_000);
    await passes(2);
    expect(billing.received.map((meta) => meta.attempt)).toEqual([1, 2]);
    const { rows } = await database.pool.query<{ payload: { attempt: number } }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.timer' ORDER BY (payload->>'attempt')::int`,
    );
    expect(rows.map((row) => row.payload.attempt)).toEqual([1, 2]);

    expect(await replica.orchestrator.handle('saga.timer', rows[0]?.payload)).toBe(SAGA_HANDLED.stale);
    const waiting = await instanceOf(replica, 'p-o');
    expect({ status: waiting.status, attempt: waiting.attempt }).toEqual({
      status: SAGA_STATUS.running,
      attempt: 2,
    });
    expect(await replica.orchestrator.handle('saga.timer', rows[1]?.payload)).toBe(
      SAGA_HANDLED.failureRecorded,
    );
    expect((await instanceOf(replica, 'p-o')).status).toBe(SAGA_STATUS.compensating);
  });

  it('поздний успех КОМПЕНСАЦИИ у застрявшего — late-reply в журнале; ручной повтор получает ответ на дубль и доводит', async () => {
    // Прежде late-reply писался только для фазы execute: возврат, сделанный
    // участником, у stuck-экземпляра терялся молча, а участник по образцу
    // README на дубль stepKey не отвечал — ручной повтор снова застревал.
    // Исход списания неизвестен (таймауты) — компенсируется и сам шаг: возврат.
    billing.execute.push('silent', 'silent');
    billing.compensate.push('silent', 'silent');
    await replica.start(definition, 'p-refund', { orderId: 'r' });
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'p-refund')).status).toBe(SAGA_STATUS.stuck);
    const refund = lastCommand('compensate');
    // Отмена несёт ключ выполнения: участник найдёт, что отменять, хотя исход списания неизвестен.
    expect(refund.executeStepKey).toBe(billing.received.find((meta) => meta.phase === 'execute')?.stepKey);

    expect(await replica.orchestrator.handleReply(sagaReplyOf(refund, { ok: true }))).toBe(
      SAGA_HANDLED.stale,
    );
    const shown = await replica.admin.show('p-refund');
    expect(shown?.journal.at(-1)).toEqual(
      expect.objectContaining({ event: 'late-reply', phase: 'compensate', step: 'charge' }),
    );

    await replica.admin.retry('p-refund', 'возврат подтверждён');
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'p-refund')).status).toBe(SAGA_STATUS.compensated);
  });

  it('успех, пришедший после начала компенсаций, — stale и запись late-reply в журнале', async () => {
    billing.execute.push('silent', 'silent');
    billing.compensate.push('silent', 'silent');
    await replica.start(definition, 'p-x', { orderId: 'x' });
    await passes(3);
    const first = lastCommand('execute');
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'p-x')).status).toBe(SAGA_STATUS.stuck);

    expect(
      await replica.orchestrator.handleReply(sagaReplyOf(first, { ok: true, data: { chargeId: 'поздно' } })),
    ).toBe(SAGA_HANDLED.stale);
    expect((await journalOf(replica, 'p-x')).at(-1)).toBe('late-reply:charge');
    expect((await instanceOf(replica, 'p-x')).status).toBe(SAGA_STATUS.stuck);
  });

  it('попытка 1 без ответа (исход неизвестен), попытка 2 — окончательный отказ: компенсируется и сам удалённый шаг', async () => {
    billing.execute.push('silent', 'reject');
    await replica.start(definition, 'p-silent-reject', { orderId: '1' });
    await pump(clock, [replica]);

    const journal = await journalOf(replica, 'p-silent-reject');
    expect(journal).toEqual([
      'saga-started',
      'step-completed:reserve',
      'step-dispatched:charge',
      'step-timed-out:charge',
      'step-dispatched:charge',
      'step-failed:charge',
      'step-dispatched:charge',
      'step-compensated:charge',
      'step-compensated:reserve',
      'saga-compensated',
    ]);
    expect(billing.received.filter((meta) => meta.phase === 'compensate')).toHaveLength(1);
    // Двойник: окончательный отказ с первой попытки (исход известен) — шаг не компенсируется.
    billing.execute.push('reject');
    await replica.start(definition, 'p-reject', { orderId: '2' });
    await pump(clock, [replica]);
    expect((await journalOf(replica, 'p-reject')).filter((entry) => entry.endsWith(':charge'))).toEqual([
      'step-dispatched:charge',
      'step-failed:charge',
    ]);
  });
});
