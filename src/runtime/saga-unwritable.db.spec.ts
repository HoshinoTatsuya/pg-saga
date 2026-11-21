import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
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
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import type { SagaRemoteCommand } from '../types/saga-remote-command.type';
import { SagaOrchestrator } from './saga-orchestrator';

/**
 * Команда участнику, которую outbox не запишет НИКОГДА, — по ВСЕМУ классу
 * отказов записи (проверка события до записи и отказ данных на сервере), а не
 * по двум известным случаям: отказ шага без повторов и компенсации, а не
 * повторы доставки до DEAD и вечный running.
 */
describeDb('SagaOrchestrator: команда, которую outbox не запишет', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let cases = 0;

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

  const sagaWith = (name: string, command: () => SagaRemoteCommand) =>
    defineSaga<{ orderId: string }>(name, { version: 1 })
      .step('reserve', {
        execute: async (args) => {
          await recordEffect(args.tx, args.sagaId, 'reserve', 'execute', args.attempt);
        },
        compensate: (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'compensate', args.attempt),
      })
      .remoteStep('charge', { command, timeoutMs: 30_000, retry: { maxAttempts: 3 } })
      .build();

  it.each([
    [
      'payload больше maxPayloadBytes (1 МиБ)',
      () => ({ eventType: 'billing.charge', payload: { blob: 'x'.repeat(1_100_000) } }),
      /maxPayloadBytes/,
    ],
    [
      'заголовки без опции headers у outbox',
      () => ({ eventType: 'billing.charge', payload: {}, headers: { 'x-trace': '1' } }),
      /headers/,
    ],
    [
      'eventType длиннее varchar(64)',
      () => ({ eventType: `billing.${'c'.repeat(70)}`, payload: {} }),
      // outbox 0.6 — отказ сервера (22001), 0.7 — проверка до записи с именем поля.
      /too long|длинн|eventType — строка до 64 знаков/,
    ],
    [
      'aggregateType длиннее varchar(64)',
      () => ({ eventType: 'billing.charge', payload: {}, aggregateType: 'a'.repeat(70) }),
      /too long|длинн|aggregateType — строка до 64 знаков/,
    ],
    ['BigInt в payload', () => ({ eventType: 'billing.charge', payload: { amount: 10n } }), /JSON/],
    ['NUL в payload', () => ({ eventType: 'billing.charge', payload: { note: 'a\u0000b' } }), /NUL/],
    ['нет eventType', () => ({ payload: {} }) as unknown as SagaRemoteCommand, /нет eventType/],
  ] as const)('%s — отказ шага без повторов, компенсации, compensated', async (_name, command, reason) => {
    cases += 1;
    const id = `u-${cases}`;
    const definition = sagaWith(`unwritable-${cases}`, command);
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, id, { orderId: '1' });
    await pump(clock, [replica]);

    const instance = await instanceOf(replica, id);
    expect({
      status: instance.status,
      journal: await journalOf(replica, id),
      effects: await effectsOf(database.pool, id),
      reason: reason.test(instance.failure ?? ''),
    }).toEqual({
      status: SAGA_STATUS.compensated,
      // Одна попытка: повтор откажет так же; исход определён — команда не ушла.
      journal: [
        'saga-started',
        'step-completed:reserve',
        'step-failed:charge',
        'step-compensated:reserve',
        'saga-compensated',
      ],
      effects: ['reserve:execute', 'reserve:compensate'],
      reason: true,
    });
    // Ни одной команды участнику в outbox и ни одной доставки в DEAD.
    const { rows } = await database.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox}
        WHERE event_type NOT IN ('saga.command', 'saga.timer', 'saga.reply') OR status <> 'published'`,
    );
    expect(rows[0]?.n).toBe(0);
  });

  it('двойник: буквальный текст «\\u0000» (не NUL) в payload и контексте — команда уходит, сага доезжает', async () => {
    const definition = defineSaga<{ orderId: string }>('literal', { version: 1 })
      .step('a', { execute: async () => ({ pattern: '\\u0000' }) })
      .remoteStep('charge', {
        command: ({ context }) => ({ eventType: 'billing.charge', payload: { pattern: context.pattern } }),
        timeoutMs: 30_000,
      })
      .build();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: () => [['billing.charge', async () => undefined]],
    });
    await replica.start(definition, 'literal', { orderId: '1' });
    for (let pass = 0; pass < 6; pass += 1) {
      await replica.relay.runOnce();
    }
    const { rows } = await database.pool.query<{ pattern: string }>(
      `SELECT payload->'data'->>'pattern' AS pattern FROM ${TEST_TABLES.outbox} WHERE event_type = 'billing.charge'`,
    );
    expect({
      pattern: rows.map((row) => row.pattern),
      context: (await instanceOf(replica, 'literal')).context.pattern,
      journal: await journalOf(replica, 'literal'),
    }).toEqual({
      pattern: ['\\u0000'],
      context: '\\u0000',
      journal: ['saga-started', 'step-completed:a', 'step-dispatched:charge'],
    });
  });

  /**
   * Проводка классификации в оркестраторе (а не только юнит функции): отказ
   * БАЗЫ или СВЯЗИ при записи команды участнику — не отказ шага. Доставка
   * уходит наружу (повторит outbox), попытка шага не тратится, компенсаций нет.
   */
  it.each([
    [
      'обрыв под обёрткой drizzle (08006)',
      () =>
        Object.assign(new Error('Failed query: insert …'), {
          query: 'insert …',
          params: [],
          cause: Object.assign(new Error('connection terminated'), { code: '08006' }),
        }),
    ],
    [
      'обрыв без SQLSTATE (OutboxDatabaseError)',
      () => Object.assign(new Error('отказ базы'), { name: 'OutboxDatabaseError', operation: 'enqueue' }),
    ],
    [
      'голый TypeError своей обёртки над outbox',
      () => new TypeError("Cannot read properties of null (reading 'write')"),
    ],
  ] as const)(
    'отказ записи команды участнику: %s — не отказ шага, повтор доставки доводит сагу',
    async (_name, failure) => {
      cases += 1;
      const id = `w-${cases}`;
      const definition = sagaWith(`wire-${cases}`, () => ({ eventType: 'billing.charge', payload: {} }));
      const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
      let failures = 1;
      const orchestrator = new SagaOrchestrator({
        db: replica.db,
        outbox: {
          config: replica.outbox.config,
          enqueue: async (tx, messages, now) => {
            if (failures > 0 && messages.some((message) => message.eventType === 'billing.charge')) {
              failures -= 1;
              throw failure();
            }
            return await replica.outbox.enqueue(tx, messages, now);
          },
        },
        inbox: replica.inbox,
        tables: { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal },
        transaction: replica.transaction,
        clock: () => clock.now,
        logger: { warn: () => undefined },
      }).register(definition);
      await replica.start(definition, id, { orderId: '1' });
      await replica.relay.runOnce();
      const command = async (): Promise<Record<string, unknown>> => {
        const { rows } = await database.pool.query<{ payload: Record<string, unknown> }>(
          `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.command' AND status = 'pending'`,
        );
        return rows[0]?.payload ?? {};
      };
      const pending = await command();

      const first = await orchestrator.handle('saga.command', pending).then(
        () => 'обработано',
        () => 'отказ доставки',
      );
      const afterFailure = await instanceOf(replica, id);
      const second = await orchestrator.handle('saga.command', pending);
      expect({
        first,
        failures,
        afterFailure: {
          status: afterFailure.status,
          stepIndex: afterFailure.stepIndex,
          attempt: afterFailure.attempt,
        },
        second,
        journal: await journalOf(replica, id),
      }).toEqual({
        first: 'отказ доставки',
        failures: 0,
        afterFailure: { status: SAGA_STATUS.running, stepIndex: 1, attempt: 1 },
        second: 'dispatched',
        journal: ['saga-started', 'step-completed:reserve', 'step-dispatched:charge'],
      });
    },
  );
});
