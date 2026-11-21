import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { sagaReplyOf } from '../remote/saga-reply-of';
import { billingParticipant, newBillingScript, paymentSaga } from '../testing/billing-participant';
import {
  createReplica,
  effectsOf,
  instanceOf,
  journalOf,
  pump,
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
import { newScript, orderSaga } from '../testing/test-sagas';
import { sagaMessageId } from './saga-message-id';

const HOUR = 3_600_000;

/**
 * Воплощение (случайное при старте) — часть id сообщений И сверки «ждёт ли экземпляр
 * это сообщение»: после prune и нового старта с тем же id таймер и ответ
 * прежнего воплощения с тем же адресом новому экземпляру не применяются.
 */
describeDb('SagaOrchestrator: сообщения прежнего воплощения того же id', () => {
  let database: TestDatabase;
  let clock: TestClock;

  /** paymentSaga с таймером ответа в час и без повторов шага. */
  const definition = (() => {
    const base = paymentSaga();
    return {
      ...base,
      steps: base.steps.map((step) =>
        step.name === 'charge'
          ? { ...step, timeoutMs: HOUR, retry: { ...step.retry, maxAttempts: 1 } }
          : step,
      ),
    } as typeof base;
  })();

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
  });

  const passes = async (replica: ReturnType<typeof createReplica>, count: number): Promise<void> => {
    for (let pass = 0; pass < count; pass += 1) {
      await replica.relay.runOnce();
    }
  };

  it('таймер прежнего воплощения (срок через час) созрел, когда НОВОЕ ждёт ответа на тот же шаг, — stale', async () => {
    const billing = newBillingScript();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    // Воплощение 1 доезжает за секунды; его таймер ответа ждёт час.
    await replica.start(definition, 'order:1', { orderId: '1' });
    await passes(replica, 10);
    expect((await instanceOf(replica, 'order:1')).status).toBe(SAGA_STATUS.completed);
    clock.now = new Date(START.getTime() + 10 * 60_000);
    expect(await replica.admin.prune(new Date(clock.now.getTime() - 5 * 60_000))).toBe(1);
    // Воплощение 2: участник молчит (ответ ещё в пути).
    billing.execute.push('silent');
    await replica.start(definition, 'order:1', { orderId: '1' });
    await passes(replica, 10);
    // Час от первой отправки: созревает таймер ПРЕЖНЕГО воплощения.
    clock.now = new Date(START.getTime() + HOUR + 1_000);
    await passes(replica, 10);

    const instance = await instanceOf(replica, 'order:1');
    expect({
      status: instance.status,
      stepIndex: instance.stepIndex,
      // Ждёт своего таймера: +10 мин + час.
      wakeAt: instance.wakeAt?.getTime(),
      journal: await journalOf(replica, 'order:1'),
    }).toEqual({
      status: SAGA_STATUS.running,
      stepIndex: 1,
      wakeAt: START.getTime() + 10 * 60_000 + HOUR,
      journal: ['saga-started', 'step-completed:reserve', 'step-dispatched:charge'],
    });
    // Выборка не выродилась: таймер прежнего воплощения действительно доставлен.
    const { rows } = await database.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.timer' AND status = 'published'`,
    );
    expect(rows[0]?.n).toBe(1);
  });

  it('задержанный ответ прежнего воплощения — stale: данные не вливаются, шаг не пройден, журнал нового не тронут', async () => {
    const billing = newBillingScript();
    billing.execute.push('silent');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'order:2', { orderId: '2' });
    await passes(replica, 10);
    const old = billing.received[0];
    if (old === undefined) {
      throw new Error('участник не получил команду воплощения 1');
    }
    // Воплощение 1 — таймаут и компенсации; удалено.
    clock.now = new Date(START.getTime() + HOUR + 1_000);
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'order:2')).status).toBe(SAGA_STATUS.compensated);
    clock.now = new Date(clock.now.getTime() + 10 * 60_000);
    expect(await replica.admin.prune(new Date(clock.now.getTime() - 60_000))).toBe(1);
    billing.execute.push('silent');
    await replica.start(definition, 'order:2', { orderId: '2' });
    await passes(replica, 10);
    const fresh = billing.received.at(-1);
    // Двойник: адрес нового — тот же, что у прежнего; различает только воплощение.
    expect({ ...fresh, commandId: '', stepKey: '', incarnation: 0 }).toEqual({
      ...old,
      commandId: '',
      stepKey: '',
      incarnation: 0,
    });

    const handled = await replica.orchestrator.handleReply(
      sagaReplyOf(old, { ok: true, data: { chargeId: 'OLD-CHARGE' } }),
    );
    const instance = await instanceOf(replica, 'order:2');
    expect({
      handled,
      status: instance.status,
      stepIndex: instance.stepIndex,
      chargeId: instance.context.chargeId,
      journal: await journalOf(replica, 'order:2'),
    }).toEqual({
      handled: SAGA_HANDLED.stale,
      status: SAGA_STATUS.running,
      stepIndex: 1,
      chargeId: undefined,
      journal: ['saga-started', 'step-completed:reserve', 'step-dispatched:charge'],
    });
    // Двойник: ответ на команду НОВОГО воплощения применяется.
    if (fresh === undefined) {
      throw new Error('участник не получил команду воплощения 2');
    }
    expect(
      await replica.orchestrator.handleReply(sagaReplyOf(fresh, { ok: true, data: { chargeId: 'NEW' } })),
    ).toBe(SAGA_HANDLED.applied);
    expect((await instanceOf(replica, 'order:2')).context.chargeId).toBe('NEW');
  });

  it('тот же id после prune при НЕПОДВИЖНЫХ часах и явном now старта — новый экземпляр едет, ключи шагов разные', async () => {
    // Двойник реального использования: тесты потребителя с неподвижными часами
    // и `now` старта из времени события — момент создания у обоих воплощений
    // один и тот же. Воплощение по нему дало бы те же id сообщений: outbox
    // счёл бы первую команду дублем, inbox — уже обработанной.
    const billing = newBillingScript();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    const startAt = (id: string) =>
      replica.transaction((tx) =>
        replica.orchestrator.start(tx, definition, { id, input: { orderId: '9' } }, new Date(START)),
      );
    expect(await startAt('order:9')).toEqual({ started: true });
    await passes(replica, 10);
    const first = await instanceOf(replica, 'order:9');
    expect(first.status).toBe(SAGA_STATUS.completed);
    expect(await replica.admin.prune(new Date(START.getTime() + 1))).toBe(1);

    expect(await startAt('order:9')).toEqual({ started: true });
    await passes(replica, 10);
    const second = await instanceOf(replica, 'order:9');
    expect({
      status: second.status,
      createdAt: second.createdAt.getTime(),
      sameIncarnation: second.incarnation === first.incarnation,
      keys: [...new Set(billing.received.map((meta) => meta.stepKey))].length,
      clockMoved: clock.now.getTime() - START.getTime(),
    }).toEqual({
      status: SAGA_STATUS.completed,
      createdAt: START.getTime(),
      sameIncarnation: false,
      keys: 2,
      clockMoved: 0,
    });
  });

  it('тот же id после prune с неподвижными часами — локальная сага: шаги второго воплощения выполнены', async () => {
    const script = newScript();
    const local = orderSaga(script);
    const replica = createReplica({ pool: database.pool, clock, definitions: [local] });
    for (const round of [1, 2]) {
      await replica.start(local, 'order:local', { orderId: 'l' });
      await passes(replica, 12);
      expect({ round, status: (await instanceOf(replica, 'order:local')).status }).toEqual({
        round,
        status: SAGA_STATUS.completed,
      });
      if (round === 1) {
        expect(await replica.admin.prune(new Date(START.getTime() + 1))).toBe(1);
      }
    }
    // Эффекты обоих воплощений: каждый шаг — дважды (по разу на воплощение).
    expect(await effectsOf(database.pool, 'order:local')).toEqual([
      'reserve:execute',
      'notify:execute',
      'charge:execute',
      'ship:execute',
      'reserve:execute',
      'notify:execute',
      'charge:execute',
      'ship:execute',
    ]);
  });

  it('экземпляр, начатый 0.1.0 (ключ формата 1): летящие ответ и таймер с id её формата — применяются', async () => {
    const billing = newBillingScript();
    billing.execute.push('silent', 'silent');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'legacy-reply', { orderId: '3' });
    await replica.start(definition, 'legacy-timer', { orderId: '4' });
    await passes(replica, 12);
    // Строки, как их пишет 0.1.0: steps — массив имён.
    await database.pool.query(
      `UPDATE ${TEST_TABLES.instances} SET steps = '["reserve","charge","ship"]'::jsonb`,
    );
    const address = (sagaId: string) =>
      ({ sagaId, generation: 0, phase: 'execute', stepIndex: 1, attempt: 1, incarnation: null }) as const;
    const sent = billing.received.find((meta) => meta.sagaId === 'legacy-reply');
    if (sent === undefined) {
      throw new Error('участник не получил команду');
    }
    expect(
      await replica.orchestrator.handleReply(
        sagaReplyOf(
          { ...sent, commandId: sagaMessageId.remote(address('legacy-reply')) },
          { ok: true, data: { chargeId: 'legacy' } },
        ),
      ),
    ).toBe(SAGA_HANDLED.applied);
    const { incarnation: _none, ...timer } = address('legacy-timer');
    expect(
      await replica.orchestrator.handle('saga.timer', {
        ...timer,
        commandId: sagaMessageId.timer(address('legacy-timer')),
      }),
    ).toBe(SAGA_HANDLED.failureRecorded);
    expect({
      reply: (await instanceOf(replica, 'legacy-reply')).stepIndex,
      timer: await journalOf(replica, 'legacy-timer'),
    }).toEqual({
      reply: 2,
      timer: ['saga-started', 'step-completed:reserve', 'step-dispatched:charge', 'step-timed-out:charge'],
    });
  });

  it('двойник: экземпляр 0.2.0 ответ с id формата 0.1.0 (без воплощения) не принимает', async () => {
    const billing = newBillingScript();
    billing.execute.push('silent');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'fresh', { orderId: '4' });
    await passes(replica, 10);
    const sent = billing.received[0];
    if (sent === undefined) {
      throw new Error('участник не получил команду');
    }
    const legacyRemote = sagaMessageId.remote({
      sagaId: 'fresh',
      generation: 0,
      phase: 'execute',
      stepIndex: 1,
      attempt: 1,
      incarnation: null,
    });
    expect(
      await replica.orchestrator.handleReply(sagaReplyOf({ ...sent, commandId: legacyRemote }, { ok: true })),
    ).toBe(SAGA_HANDLED.stale);
    expect((await instanceOf(replica, 'fresh')).stepIndex).toBe(1);
  });
});
