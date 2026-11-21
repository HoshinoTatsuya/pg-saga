import { sql } from 'drizzle-orm';

import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaStateError } from '../errors/saga-state.error';
import {
  createReplica,
  effectsOf,
  instanceOf,
  journalOf,
  pump,
  type SagaReplica,
  START,
  type TestClock,
} from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  rejectionOf,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import { type FailureScript, newScript, orderSaga } from '../testing/test-sagas';
import type { SagaMetrics } from '../types/saga-metrics.type';

/** Оркестратор на настоящей базе под ролью без суперправ: локальные шаги. */
describeDb('SagaOrchestrator: локальные шаги', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let script: FailureScript;
  let replica: SagaReplica;
  let signals: string[];

  const metrics: SagaMetrics = {
    completed: (event) => signals.push(`completed:${event.sagaId}`),
    compensated: (event) => signals.push(`compensated:${event.sagaId}`),
    stuck: (event) => signals.push(`stuck:${event.sagaId}`),
    stepFailed: (event) => signals.push(`failed:${event.step}:${event.attempt}:${event.willRetry}`),
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
    script = newScript();
    signals = [];
    replica = createReplica({ pool: database.pool, clock, definitions: [orderSaga(script)], metrics });
  });

  afterEach(async () => {
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('счастливый путь: все шаги по порядку, контекст копится, журнал полный, хук completed — один раз', async () => {
    await expect(replica.start(orderSaga(script), 'o-1', { orderId: '1' })).rejects.toThrow(
      'не зарегистрирована',
    );
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition], metrics });

    expect(await local.start(definition, 'o-1', { orderId: '1' })).toBe(true);
    await pump(clock, [local]);

    const instance = await instanceOf(local, 'o-1');
    expect({
      status: instance.status,
      context: instance.context,
      finished: instance.finishedAt !== null,
    }).toEqual({
      status: SAGA_STATUS.completed,
      context: { orderId: '1', reservationId: 'r-1', chargeId: 'c-r-1', shipped: 'c-r-1' },
      finished: true,
    });
    expect(await effectsOf(database.pool, 'o-1')).toEqual([
      'reserve:execute',
      'notify:execute',
      'charge:execute',
      'ship:execute',
    ]);
    expect(await journalOf(local, 'o-1')).toEqual([
      'saga-started',
      'step-completed:reserve',
      'step-completed:notify',
      'step-completed:charge',
      'step-completed:ship',
      'saga-completed',
    ]);
    expect(signals).toEqual(['completed:o-1']);
    expect(local.warnings).toEqual([]);
  });

  it('повтор старта с тем же id ничего не делает; старт в откатившейся транзакции не оставляет ничего', async () => {
    expect(
      await replica.start(orderSaga(script), 'x', { orderId: 'x' }).catch(() => 'не зарегистрирована'),
    ).toBe('не зарегистрирована');
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });

    expect(await local.start(definition, 'o-2', { orderId: '2' })).toBe(true);
    expect(await local.start(definition, 'o-2', { orderId: 'другой' })).toBe(false);
    const failure = await rejectionOf(
      local.transaction(async (tx) => {
        await local.orchestrator.start(tx, definition, { id: 'o-3', input: { orderId: '3' } });
        throw new Error('откат вызывающего');
      }),
    );

    expect(String(failure)).toContain('откат вызывающего');
    const { rows } = await database.pool.query<{ id: string; context: unknown }>(
      `SELECT id, context FROM ${TEST_TABLES.instances} ORDER BY id`,
    );
    expect(rows).toEqual([{ id: 'o-2', context: { orderId: '2' } }]);
    const commands = await database.pool.query<{ aggregate_id: string }>(
      `SELECT aggregate_id FROM ${TEST_TABLES.outbox} ORDER BY aggregate_id`,
    );
    expect(commands.rows).toEqual([{ aggregate_id: 'o-2' }]);
  });

  it('id занят — start говорит кем: та же сага и версия или другая', async () => {
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    expect(await local.start(definition, 'o-busy', { orderId: '1' })).toBe(true);
    const again = await local.transaction((tx) =>
      local.orchestrator.start(tx, definition, { id: 'o-busy', input: { orderId: '2' } }),
    );
    expect(again).toEqual({
      started: false,
      existing: { name: 'order', version: 1, status: SAGA_STATUS.running },
    });
  });

  it('тот же id после prune — новый экземпляр едет: отметки inbox и строки outbox прежнего его не глушат', async () => {
    // Прежде id сообщений зависели только от (id, поколение, фаза, шаг,
    // попытка): первая команда нового экземпляра совпадала с командой
    // прежнего, outbox считал её дублем, inbox — уже обработанной, и start
    // отвечал started: true про сагу, которая не поедет никогда.
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    expect(await local.start(definition, 'o-again', { orderId: '1' })).toBe(true);
    await pump(clock, [local]);
    expect((await instanceOf(local, 'o-again')).status).toBe(SAGA_STATUS.completed);

    clock.now = new Date(clock.now.getTime() + 60_000);
    expect(await local.admin.prune(clock.now)).toBe(1);
    expect(await local.start(definition, 'o-again', { orderId: '2' })).toBe(true);
    await pump(clock, [local]);

    expect({
      status: (await instanceOf(local, 'o-again')).status,
      journal: await journalOf(local, 'o-again'),
    }).toEqual({
      status: SAGA_STATUS.completed,
      journal: [
        'saga-started',
        'step-completed:reserve',
        'step-completed:notify',
        'step-completed:charge',
        'step-completed:ship',
        'saga-completed',
      ],
    });
  });

  /**
   * Отказ на каждом шаге: компенсируются РОВНО завершённые шаги, у которых
   * есть компенсация, и в ОБРАТНОМ порядке. Эффекты отказавшего шага
   * откатились вместе с его транзакцией.
   */
  it.each([
    ['reserve', [], []],
    ['notify', ['reserve:execute'], ['reserve']],
    ['charge', ['reserve:execute', 'notify:execute'], ['reserve']],
    ['ship', ['reserve:execute', 'notify:execute', 'charge:execute'], ['charge', 'reserve']],
  ] as const)(
    'отказ шага %s → компенсации завершённых в обратном порядке',
    async (failing, executed, compensated) => {
      const definition = orderSaga(script);
      const local = createReplica({ pool: database.pool, clock, definitions: [definition], metrics });
      script.execute.set(failing, 'always');

      await local.start(definition, 'o-f', { orderId: 'f' });
      await pump(clock, [local]);

      const effects = await effectsOf(database.pool, 'o-f');
      const compensations = effects.filter((effect) => effect.endsWith(':compensate'));
      // Множество — отдельно от порядка: лишняя или пропущенная компенсация видна сама по себе.
      expect(new Set(compensations)).toEqual(new Set(compensated.map((step) => `${step}:compensate`)));
      expect(effects).toEqual([...executed, ...compensated.map((step) => `${step}:compensate`)]);
      const instance = await instanceOf(local, 'o-f');
      expect({ status: instance.status, failure: instance.failure }).toEqual({
        status: SAGA_STATUS.compensated,
        failure: `Error: ${failing}: отказ попытки 3`,
      });
      const journal = await journalOf(local, 'o-f');
      expect(journal.filter((entry) => entry === `step-failed:${failing}`)).toHaveLength(3);
      expect(journal.filter((entry) => entry.startsWith('step-compensated:'))).toEqual(
        compensated.map((step) => `step-compensated:${step}`),
      );
      expect(journal.at(-1)).toBe('saga-compensated');
      expect(signals).toEqual([
        `failed:${failing}:1:true`,
        `failed:${failing}:2:true`,
        `failed:${failing}:3:false`,
        'compensated:o-f',
      ]);
    },
  );

  it('повтор с паузой: два отказа, третья попытка проходит; паузы — по backoff, эффект шага один', async () => {
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    script.execute.set('charge', 2);

    await local.start(definition, 'o-r', { orderId: 'r' });
    await pump(clock, [local]);

    expect((await instanceOf(local, 'o-r')).status).toBe(SAGA_STATUS.completed);
    expect((await effectsOf(database.pool, 'o-r')).filter((effect) => effect.startsWith('charge'))).toEqual([
      'charge:execute',
    ]);
    const { rows } = await database.pool.query<{ attempt: number; delay: number }>(
      `SELECT (payload->>'attempt')::int AS attempt,
              (extract(epoch FROM next_attempt_at - created_at) * 1000)::int AS delay
         FROM ${TEST_TABLES.outbox}
        WHERE event_type = 'saga.command' AND payload->>'stepIndex' = '2'
        ORDER BY attempt`,
    );
    // random = 0.5: половина паузы постоянная, половина — 0.5 от неё: 750 и 1500 мс.
    expect(rows).toEqual([
      { attempt: 1, delay: 0 },
      { attempt: 2, delay: 750 },
      { attempt: 3, delay: 1500 },
    ]);
  });

  it('SagaStepRejectedError — без повторов: одна попытка, сразу компенсации', async () => {
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition], metrics });
    script.execute.set('charge', 'reject');

    await local.start(definition, 'o-j', { orderId: 'j' });
    await pump(clock, [local]);

    expect(await journalOf(local, 'o-j')).toEqual([
      'saga-started',
      'step-completed:reserve',
      'step-completed:notify',
      'step-failed:charge',
      'step-compensated:reserve',
      'saga-compensated',
    ]);
    expect(signals).toEqual(['failed:charge:1:false', 'compensated:o-j']);
  });

  it('локальный шаг дольше timeoutMs — отказ по таймауту с повторами, затем компенсации', async () => {
    const definition = orderSaga(script, 1, 150);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    script.execute.set('reserve', 'hang');

    await local.start(definition, 'o-t', { orderId: 't' });
    await pump(clock, [local]);

    const instance = await instanceOf(local, 'o-t');
    expect({ status: instance.status, failure: instance.failure }).toEqual({
      status: SAGA_STATUS.compensated,
      failure: 'SagaStepTimeoutError: шаг не уложился в 150 мс',
    });
    expect(
      (await journalOf(local, 'o-t')).filter((entry) => entry === 'step-timed-out:reserve'),
    ).toHaveLength(3);
    expect(await effectsOf(database.pool, 'o-t')).toEqual([]);
  });

  it('упала компенсация: повторы, затем stuck (хук stuck — один раз); ручной повтор доводит до compensated', async () => {
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition], metrics });
    script.execute.set('charge', 'reject');
    script.compensate.set('reserve', 'always');

    await local.start(definition, 'o-s', { orderId: 's' });
    await pump(clock, [local]);

    const stuck = await instanceOf(local, 'o-s');
    expect({ status: stuck.status, step: stuck.steps[stuck.stepIndex], attempt: stuck.attempt }).toEqual({
      status: SAGA_STATUS.stuck,
      step: 'reserve',
      attempt: 3,
    });
    expect((await journalOf(local, 'o-s')).slice(-4)).toEqual([
      'compensation-failed:reserve',
      'compensation-failed:reserve',
      'compensation-failed:reserve',
      'saga-stuck',
    ]);
    expect(signals.filter((signal) => signal.startsWith('stuck'))).toEqual(['stuck:o-s']);
    expect(await local.admin.stuckReport(10, clock.now)).toEqual([
      {
        id: 'o-s',
        saga: 'order',
        version: 1,
        status: SAGA_STATUS.stuck,
        step: 'reserve',
        attempt: 3,
        failure: 'SagaStepRejectedError: charge: окончательный отказ',
        lastError: 'Error: reserve: отказ попытки 3',
        stuckForMs: 0,
      },
    ]);
    // Застрявший сам не двигается: команд в outbox не осталось.
    expect((await pump(clock, [local])).rounds).toBe(1);

    script.compensate.delete('reserve');
    await local.admin.retry('o-s', 'склад поднят');
    await pump(clock, [local]);

    const done = await instanceOf(local, 'o-s');
    expect({ status: done.status, generation: done.generation }).toEqual({
      status: SAGA_STATUS.compensated,
      generation: 1,
    });
    expect((await journalOf(local, 'o-s')).slice(-3)).toEqual([
      'manual-retry:reserve',
      'step-compensated:reserve',
      'saga-compensated',
    ]);
    expect(await effectsOf(database.pool, 'o-s')).toEqual([
      'reserve:execute',
      'notify:execute',
      'reserve:compensate',
    ]);
  });

  it('компенсация отказала дважды и прошла — не stuck; resolve и retry только для stuck', async () => {
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    script.execute.set('notify', 'reject');
    script.compensate.set('reserve', 2);

    await local.start(definition, 'o-c', { orderId: 'c' });
    await pump(clock, [local]);

    expect((await instanceOf(local, 'o-c')).status).toBe(SAGA_STATUS.compensated);
    const refused = await rejectionOf(local.admin.retry('o-c'));
    expect(refused).toBeInstanceOf(SagaStateError);
    await expect(local.admin.resolve('o-c', 'нечего разбирать')).rejects.toBeInstanceOf(SagaStateError);
    await expect(local.admin.retry('нет-такого')).rejects.toThrow('экземпляра нет-такого нет');

    script.execute.set('notify', 'reject');
    script.compensate.set('reserve', 'always');
    await local.start(definition, 'o-z', { orderId: 'z' });
    await pump(clock, [local]);
    await local.admin.resolve('o-z', 'возврат проведён вручную');

    const resolved = await local.admin.show('o-z');
    expect({ status: resolved?.instance.status, last: resolved?.journal.at(-1) }).toEqual({
      status: SAGA_STATUS.resolved,
      last: expect.objectContaining({
        event: 'manual-resolve',
        detail: 'возврат проведён вручную',
        step: 'reserve',
      }),
    });
  });

  it('timeoutMs локального шага — и потолок SQL шага (statement_timeout); шаг без своего — потолок по умолчанию', async () => {
    // Прежде таймаут отменял только ожидание в процессе: pg_sleep(3) при
    // timeoutMs 200 держал строку экземпляра и соединение все 3 секунды.
    const seen: string[] = [];
    script.inside = async (step, args) => {
      const result = await args.tx.execute(sql`SELECT current_setting('statement_timeout') AS value`);
      seen.push(`${step}:${String((result.rows[0] as { value: string }).value)}`);
    };
    const definition = orderSaga(script, 1, 200);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await local.start(definition, 'o-sql-timeout', { orderId: '1' });
    await pump(clock, [local]);

    expect((await instanceOf(local, 'o-sql-timeout')).status).toBe(SAGA_STATUS.completed);
    // Потолок запроса — остаток бюджета шага: не больше потолка шага и почти весь
    // он у первого запроса (шаг только начат).
    const ms = seen.slice(0, 2).map((entry) => {
      const [step, value] = entry.split(':') as [string, string];
      const n = Number.parseInt(value, 10) * (value.endsWith('ms') ? 1 : 1_000);
      return [step, n];
    });
    expect(ms.map(([step]) => step)).toEqual(['reserve', 'notify']);
    for (const [[, n], limit] of [
      [ms[0] as [string, number], 200],
      [ms[1] as [string, number], 12_000],
    ] as const) {
      expect(n).toBeLessThanOrEqual(limit);
      expect(n).toBeGreaterThan(limit - 150);
    }
  });

  it('SQL шага дольше timeoutMs снимается сервером: отказ шага по таймауту, а не ожидание конца запроса', async () => {
    script.inside = async (step, args) => {
      if (step === 'reserve' && args.attempt === 1) {
        await args.tx.execute(sql`SELECT pg_sleep(30)`);
      }
    };
    const definition = orderSaga(script, 1, 200);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await local.start(definition, 'o-sql-cancel', { orderId: '1' });
    await pump(clock, [local]);

    // pg_sleep(30) без потолка сервера держал бы прогон 30 с (потолок теста — 15 с).
    const instance = await instanceOf(local, 'o-sql-cancel');
    expect({ status: instance.status, failed: (await journalOf(local, 'o-sql-cancel'))[1] }).toEqual({
      status: SAGA_STATUS.completed,
      failed: 'step-timed-out:reserve',
    });
  });

  it('шаг поймал ошибку своего SQL и вернулся (25P02) — отказ шага с повтором, а не повторы доставки до DEAD', async () => {
    script.inside = async (step, args) => {
      if (step === 'reserve' && args.attempt === 1) {
        try {
          await args.tx.execute(sql`SELECT 1 / 0`);
        } catch {
          // Шаг «обработал» ошибку и продолжил — транзакция при этом прервана.
        }
      }
    };
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await local.start(definition, 'o-swallowed', { orderId: '1' });
    await pump(clock, [local]);

    const shown = await local.admin.show('o-swallowed');
    expect({
      status: shown?.instance.status,
      failed: shown?.journal.find((entry) => entry.event === 'step-failed')?.detail,
    }).toEqual({
      status: SAGA_STATUS.completed,
      failed: expect.stringContaining('проглотил ошибку SQL: транзакция прервана (25P02)'),
    });
  });

  it('время перехода — после шага: пауза повтора от момента отказа, finished_at включает последний шаг', async () => {
    // Прежде `now` снимался ДО шага: шаг 15 с при паузе ~0,75 с ставил повтор
    // в прошлое (сразу), а finished_at и длительность саги не видели последнего шага.
    script.execute.set('reserve', 1);
    let failedAt = 0;
    let shipEnd = 0;
    script.inside = async (step, args) => {
      if (step === 'reserve' && args.attempt === 1) {
        clock.now = new Date(clock.now.getTime() + 15_000);
        failedAt = clock.now.getTime();
      }
      if (step === 'ship') {
        clock.now = new Date(clock.now.getTime() + 5_000);
        shipEnd = clock.now.getTime();
      }
    };
    const definition = orderSaga(script);
    const local = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await local.start(definition, 'o-time', { orderId: '1' });
    await pump(clock, [local]);

    const { rows } = await database.pool.query<{ next: Date }>(
      `SELECT next_attempt_at AS next FROM ${TEST_TABLES.outbox}
        WHERE event_type = 'saga.command' AND (payload->>'attempt')::int = 2`,
    );
    const instance = await instanceOf(local, 'o-time');
    expect({
      retryAfterFailure: (rows[0]?.next.getTime() ?? 0) > failedAt,
      finishedAtShipEnd: instance.finishedAt?.getTime() === shipEnd,
    }).toEqual({ retryAfterFailure: true, finishedAtShipEnd: true });
  });

  it('хук stepCompleted — по каждому выполненному шагу, после фиксации', async () => {
    const completed: string[] = [];
    const definition = orderSaga(script);
    const local = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      metrics: { stepCompleted: (event) => completed.push(`${event.step}:${event.phase}`) },
    });
    await local.start(definition, 'o-steps', { orderId: '1' });
    await pump(clock, [local]);
    expect(completed).toEqual(['reserve:execute', 'notify:execute', 'charge:execute', 'ship:execute']);
  });

  it('хук метрик, бросающий исключение, не ломает сагу', async () => {
    const definition = orderSaga(script);
    const local = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      metrics: {
        completed: () => {
          throw new Error('метрики недоступны');
        },
      },
    });

    await local.start(definition, 'o-m', { orderId: 'm' });
    await pump(clock, [local]);

    expect((await instanceOf(local, 'o-m')).status).toBe(SAGA_STATUS.completed);
    expect(local.warnings).toEqual([]);
  });

  it('прямой вызов handle: чужой тип события и payload не той формы — отказ; экземпляра нет — stale', async () => {
    await expect(replica.orchestrator.handle('other.event', {})).rejects.toThrow('не принадлежит саге');
    await expect(replica.orchestrator.handle('saga.command', { sagaId: 'x' })).rejects.toThrow('commandId');
    await expect(
      replica.orchestrator.handle('saga.command', {
        commandId: '6f1d3b52-a8e4-4c0f-9b7e-2d5a91c3e7f4',
        sagaId: 'нет-такого',
        generation: 0,
        phase: 'execute',
        stepIndex: 0,
        attempt: 1,
      }),
    ).resolves.toBe(SAGA_HANDLED.stale);
  });
});
