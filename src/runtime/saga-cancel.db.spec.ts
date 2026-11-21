import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { billingParticipant, newBillingScript } from '../testing/billing-participant';
import {
  createReplica,
  effectsOf,
  instanceOf,
  journalOf,
  pump,
  recordEffect,
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
  waitForLockWaiters,
} from '../testing/saga-test-database';
import { sagaMessageId } from './saga-message-id';

/** Отмена идущей саги: что компенсируется, что пишется в журнал, когда зовётся хук. */
describeDb('SagaOrchestrator.cancel', () => {
  let database: TestDatabase;
  let clock: TestClock;

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

  /** a и b — локальные с компенсацией; b ждёт `gate` (если задан) и отказывает. */
  const twoLocal = (gate?: { entered: () => void; release: Promise<void> }) =>
    defineSaga<{ orderId: string }>('two-local', { version: 1 })
      .step('a', {
        execute: async (args) => {
          await recordEffect(args.tx, args.sagaId, 'a', 'execute', args.attempt);
        },
        compensate: (args) => recordEffect(args.tx, args.sagaId, 'a', 'compensate', args.attempt),
      })
      .step('b', {
        execute: async (args) => {
          await recordEffect(args.tx, args.sagaId, 'b', 'execute', args.attempt);
          if (gate !== undefined) {
            gate.entered();
            await gate.release;
            throw new Error('b отказал');
          }
        },
        compensate: (args) => recordEffect(args.tx, args.sagaId, 'b', 'compensate', args.attempt),
      })
      .build();

  it('на локальном шаге с компенсацией, который ещё не выполнен, — компенсируется только выполненный (a), а не он (b)', async () => {
    const definition = twoLocal();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'c-local', { orderId: '1' });
    await replica.relay.runOnce();
    expect((await instanceOf(replica, 'c-local')).stepIndex).toBe(1);
    await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-local', 'клиент'));
    await pump(clock, [replica]);

    expect({
      status: (await instanceOf(replica, 'c-local')).status,
      effects: await effectsOf(database.pool, 'c-local'),
    }).toEqual({ status: SAGA_STATUS.compensated, effects: ['a:execute', 'a:compensate'] });
  });

  it('удалённый шаг БЕЗ компенсации, команда ушла: uncompensated-uncertain в журнале, как при таймауте', async () => {
    const billing = newBillingScript();
    billing.execute.push('silent');
    const definition = defineSaga<{ orderId: string }>('bare-remote', { version: 1 })
      .step('reserve', {
        execute: async (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'execute', args.attempt),
        compensate: (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'compensate', args.attempt),
      })
      .remoteStep('charge', {
        command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
        timeoutMs: 60_000,
      })
      .build();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'c-bare', { orderId: '1' });
    for (let pass = 0; pass < 6; pass += 1) {
      await replica.relay.runOnce();
    }
    expect(billing.received.length).toBe(1);
    await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-bare', 'передумал'));
    await pump(clock, [replica]);

    expect(await journalOf(replica, 'c-bare')).toEqual([
      'saga-started',
      'step-completed:reserve',
      'step-dispatched:charge',
      'saga-cancelled',
      'uncompensated-uncertain:charge',
      'step-compensated:reserve',
      'saga-compensated',
    ]);
  });

  it('компенсировать нечего: compensated сразу, а хук compensated — один раз, после фиксации оркестратора; с компенсациями — тоже один', async () => {
    const events: string[] = [];
    const definition = twoLocal();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      metrics: {
        compensated: (event) => events.push(`compensated:${event.sagaId}:${String(event.failure)}`),
      },
    });
    // c-full: шаг a выполнен (компенсировать есть что); c-fresh: ещё ничего.
    await replica.start(definition, 'c-full', { orderId: '2' });
    await replica.relay.runOnce();
    await replica.start(definition, 'c-fresh', { orderId: '3' });
    const empty = await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-fresh', 'передумал'));
    const full = await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-full', 'передумал'));
    expect({ empty, full, before: events }).toEqual({
      empty: { cancelled: true, status: SAGA_STATUS.compensated },
      full: { cancelled: true, status: SAGA_STATUS.compensating },
      before: [],
    });
    await pump(clock, [replica]);

    expect(events.sort()).toEqual([
      'compensated:c-fresh:отменено: передумал',
      'compensated:c-full:отменено: передумал',
    ]);
    expect(await journalOf(replica, 'c-fresh')).toEqual([
      'saga-started',
      'saga-cancelled',
      'saga-compensated',
    ]);
  });

  it.each([
    SAGA_STATUS.compensating,
    SAGA_STATUS.completed,
    SAGA_STATUS.compensated,
    SAGA_STATUS.stuck,
    SAGA_STATUS.resolved,
  ])(
    'не running (%s) — ничего не меняет: ни поколения, ни статуса, ни журнала, ни команды',
    async (status) => {
      const definition = twoLocal();
      const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
      await replica.start(definition, 'c-idle', { orderId: '1' });
      await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET status = $1`, [status]);
      const outboxBefore = (await database.pool.query(`SELECT id FROM ${TEST_TABLES.outbox}`)).rowCount;

      const result = await replica.transaction((tx) =>
        replica.orchestrator.cancel(tx, 'c-idle', 'передумал'),
      );
      const instance = await instanceOf(replica, 'c-idle');
      expect({
        result,
        status: instance.status,
        generation: instance.generation,
        journal: await journalOf(replica, 'c-idle'),
        outbox: (await database.pool.query(`SELECT id FROM ${TEST_TABLES.outbox}`)).rowCount,
      }).toEqual({
        result: { cancelled: false, status },
        status,
        generation: 0,
        journal: ['saga-started'],
        outbox: outboxBefore,
      });
    },
  );

  it('экземпляра нет — { cancelled: false, status: null }', async () => {
    const replica = createReplica({ pool: database.pool, clock, definitions: [twoLocal()] });
    expect(await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-none', 'передумал'))).toEqual(
      {
        cancelled: false,
        status: null,
      },
    );
  });

  it('уведомление отмены сверяет поколение: команда того же адреса ПРЕЖНЕГО поколения — stale, хук не зовётся', async () => {
    const events: string[] = [];
    const definition = twoLocal();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      metrics: { compensated: (event) => events.push(event.sagaId) },
    });
    await replica.start(definition, 'c-notice', { orderId: '1' });
    await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-notice', 'передумал'));
    const instance = await instanceOf(replica, 'c-notice');
    expect({ status: instance.status, generation: instance.generation }).toEqual({
      status: SAGA_STATUS.compensated,
      generation: 1,
    });
    // Адрес уведомления (компенсация, шаг и попытка отмены), но поколение 0 —
    // и id, который это воплощение выдало бы для поколения 0.
    const address = {
      sagaId: 'c-notice',
      generation: 0,
      phase: 'compensate',
      stepIndex: 0,
      attempt: 1,
    } as const;
    const handled = await replica.orchestrator.handle('saga.command', {
      ...address,
      commandId: sagaMessageId.command({ ...address, incarnation: instance.incarnation }),
    });
    expect({ handled, events }).toEqual({ handled: 'stale', events: [] });
    // Двойник: настоящее уведомление — хук один раз.
    await pump(clock, [replica]);
    expect(events).toEqual(['c-notice']);
  });

  it('новое поколение сверх int4 — SagaConfigError до записи, а не отказ сервера в транзакции вызывающего', async () => {
    const definition = twoLocal();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'c-int4', { orderId: '1' });
    await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET generation = 2147483647`);
    const error = await rejectionOf(
      replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-int4', 'передумал')),
    );
    expect(error).toBeInstanceOf(SagaConfigError);
    expect(await journalOf(replica, 'c-int4')).toEqual(['saga-started']);
  });

  it('отказ шага во время отмены: отмена дождалась строки и прошла — отказ откатившегося шага уже не записывается', async () => {
    let entered: () => void = () => undefined;
    const inside = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const definition = twoLocal({ entered: () => entered(), release: gate });
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'c-race', { orderId: '1' });
    await replica.relay.runOnce();
    const stepRun = replica.relay.runOnce();
    await inside;
    const cancelRun = replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-race', 'передумал'));
    await waitForLockWaiters(database.pool, 'FOR UPDATE', 1);
    release();
    await stepRun;
    expect(await cancelRun).toEqual({ cancelled: true, status: SAGA_STATUS.compensating });
    await pump(clock, [replica]);

    expect({
      journal: await journalOf(replica, 'c-race'),
      effects: await effectsOf(database.pool, 'c-race'),
    }).toEqual({
      journal: [
        'saga-started',
        'step-completed:a',
        'saga-cancelled',
        'step-compensated:a',
        'saga-compensated',
      ],
      effects: ['a:execute', 'a:compensate'],
    });
  });

  it('свой момент отмены раньше последнего сдвига — отметка не уходит назад (updated_at, журнал, finished_at не раньше created_at); Invalid Date — отказ до записи', async () => {
    const definition = twoLocal();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'c-at', { orderId: '1' });
    await replica.relay.runOnce();
    const before = await instanceOf(replica, 'c-at');
    const past = new Date('2000-01-01T00:00:00.000Z');
    await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-at', 'клиент', past));
    const after = await instanceOf(replica, 'c-at');
    const cancelled = (await replica.admin.show('c-at'))?.journal.find(
      (entry) => entry.event === 'saga-cancelled',
    );
    expect({
      updated: after.updatedAt.getTime() >= before.updatedAt.getTime(),
      journal: cancelled?.at.getTime() === after.updatedAt.getTime(),
      notBeforeCreated: after.updatedAt.getTime() >= after.createdAt.getTime(),
    }).toEqual({ updated: true, journal: true, notBeforeCreated: true });

    await replica.start(definition, 'c-nan', { orderId: '2' });
    await expect(
      replica.transaction((tx) => replica.orchestrator.cancel(tx, 'c-nan', 'клиент', new Date(Number.NaN))),
    ).rejects.toBeInstanceOf(SagaConfigError);
    expect((await instanceOf(replica, 'c-nan')).status).toBe(SAGA_STATUS.running);
  });
});
