import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaDefinitionUnavailableError } from '../errors/saga-definition-unavailable.error';
import { sagaReplyOf } from '../remote/saga-reply-of';
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
} from '../testing/saga-test-database';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';

/** Определение с шагами по именам: эффект — имя шага. */
const sagaOf = (version: number, steps: readonly string[]) => {
  let builder = defineSaga<{ orderId: string }>('versioned', { version });
  for (const name of steps) {
    builder = builder.step(name, {
      execute: async (args) => {
        await recordEffect(args.tx, args.sagaId, name, 'execute', args.attempt);
      },
    });
  }
  return builder.build();
};

/**
 * Экземпляр доезжает по версии, с которой начат. Процесс без этой версии (или
 * с разошедшимися шагами под той же версией) экземпляр не трогает и отказывает
 * доставке с причиной — outbox повторит её там, где версия есть.
 */
describeDb('SagaOrchestrator: версии определения', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let unavailable: string[];

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
    unavailable = [];
  });

  const metrics = {
    definitionUnavailable: (event: { saga: string; version: number; reason: string }) => {
      unavailable.push(`${event.saga}@${event.version}: ${event.reason}`);
    },
  };

  it('вид шага сменили без новой версии (удалённый → локальный): ответ участника — явная причина и метрика, а не безликий отказ', async () => {
    const remote = defineSaga<{ orderId: string }>('versioned', { version: 1 })
      .step('a', { execute: async () => undefined })
      .remoteStep('c', { command: () => ({ eventType: 'billing.charge', payload: {} }), timeoutMs: 60_000 })
      .build();
    const old = createReplica({ pool: database.pool, clock, definitions: [remote] });
    await old.start(remote, 'v-kind', { orderId: '1' });
    for (let round = 0; round < 5; round += 1) {
      await old.relay.runOnce();
    }
    const { rows } = await database.pool.query<{ payload: { sagaCommand: SagaCommandMeta } }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'billing.charge'`,
    );
    const meta = rows[0]?.payload.sagaCommand;
    if (meta === undefined) {
      throw new Error('команда участнику не записана');
    }

    const local = sagaOf(1, ['a', 'c']);
    const changed = createReplica({ pool: database.pool, clock, definitions: [local], metrics });
    const refused = await rejectionOf(changed.orchestrator.handleReply(sagaReplyOf(meta, { ok: true })));

    expect(refused).toBeInstanceOf(SagaDefinitionUnavailableError);
    expect(unavailable).toEqual([expect.stringContaining('вид шага сменили без новой версии')]);
  });

  it('новая версия кода не ломает летящий экземпляр: без v1 — отказ доставки и экземпляр не тронут; с v1 — доезжает по v1', async () => {
    const v1 = sagaOf(1, ['a', 'b', 'c']);
    const old = createReplica({ pool: database.pool, clock, definitions: [v1] });
    await old.start(v1, 'v-1', { orderId: '1' });
    await old.relay.runOnce();

    const v2 = sagaOf(2, ['a', 'x', 'b', 'c']);
    const fresh = createReplica({ pool: database.pool, clock, definitions: [v2], metrics });
    const { rows } = await database.pool.query<{ payload: unknown }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE status = 'pending' AND event_type = 'saga.command'`,
    );
    const refused = await rejectionOf(fresh.orchestrator.handle('saga.command', rows[0]?.payload));

    expect(refused).toBeInstanceOf(SagaDefinitionUnavailableError);
    expect(String(refused)).toContain('версия не зарегистрирована в этом процессе (есть: 2)');
    expect(unavailable).toEqual([expect.stringMatching(/^versioned@1: версия не зарегистрирована/)]);
    const untouched = await instanceOf(fresh, 'v-1');
    expect({ status: untouched.status, step: untouched.steps[untouched.stepIndex] }).toEqual({
      status: SAGA_STATUS.running,
      step: 'b',
    });

    const both = createReplica({ pool: database.pool, clock, definitions: [v1, v2] });
    await both.start(v2, 'v-2', { orderId: '2' });
    await pump(clock, [both]);

    expect(await effectsOf(database.pool, 'v-1')).toEqual(['a:execute', 'b:execute', 'c:execute']);
    expect(await effectsOf(database.pool, 'v-2')).toEqual([
      'a:execute',
      'x:execute',
      'b:execute',
      'c:execute',
    ]);
    expect((await instanceOf(both, 'v-1')).version).toBe(1);
    expect(await journalOf(both, 'v-1')).toEqual([
      'saga-started',
      'step-completed:a',
      'step-completed:b',
      'step-completed:c',
      'saga-completed',
    ]);
  });

  it('шаги поменяли без смены версии — явный отказ «разошлись», экземпляр не двигается по чужим шагам', async () => {
    const v1 = sagaOf(1, ['a', 'b', 'c']);
    const old = createReplica({ pool: database.pool, clock, definitions: [v1] });
    await old.start(v1, 'v-3', { orderId: '3' });
    await old.relay.runOnce();

    const renamed = sagaOf(1, ['a', 'b2', 'c']);
    const fresh = createReplica({ pool: database.pool, clock, definitions: [renamed], metrics });
    await fresh.relay.runOnce();

    expect(unavailable).toEqual([
      'versioned@1: шаги определения [a, b2, c] разошлись с записанными в экземпляре v-3 [a, b, c] — правка шагов требует новой версии',
    ]);
    expect(await effectsOf(database.pool, 'v-3')).toEqual(['a:execute']);
    const { rows } = await database.pool.query<{ status: string; last_error: string }>(
      `SELECT status, last_error FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.command' AND status <> 'published'`,
    );
    expect(rows).toEqual([{ status: 'pending', last_error: expect.stringContaining('разошлись') }]);
  });

  it('конечному экземпляру определение не нужно: сообщение старой версии после завершения — stale, а не отказ', async () => {
    const v1 = sagaOf(1, ['a']);
    const old = createReplica({ pool: database.pool, clock, definitions: [v1] });
    await old.start(v1, 'v-4', { orderId: '4' });
    const { rows } = await database.pool.query<{ payload: unknown }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.command'`,
    );
    await pump(clock, [old]);

    const fresh = createReplica({ pool: database.pool, clock, definitions: [sagaOf(2, ['z'])] });
    await database.pool.query(`TRUNCATE ${TEST_TABLES.inbox}`);

    expect(await fresh.orchestrator.handle('saga.command', rows[0]?.payload)).toBe('stale');
  });
});
