import { OUTBOX_ORDERING, OUTBOX_TIMESTAMP, outboxTableSql } from '@hoshinotatsuya/outbox';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { sagaReplyMessage } from '../remote/saga-reply-message';
import { sagaReplyOf } from '../remote/saga-reply-of';
import { sagaMessageId } from '../runtime/saga-message-id';
import { billingParticipant, newBillingScript, paymentSaga } from '../testing/billing-participant';
import { legacyV030TablesSql } from '../testing/legacy-v030-tables';
import {
  createReplica,
  instanceOf,
  pump,
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
import { newScript, orderSaga } from '../testing/test-sagas';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import { sagaTablesSql } from './saga-tables-sql';
import { sagaUpgradeSql } from './saga-upgrade-sql';

const MINUTE = 60_000;
/** Инструкции схемы 0.4.0 (CHECK ×2 с проверкой, индекс): строк они не меняют. */
const SCHEMA_STATEMENTS = [0, 0, 0, 0, 0];

/**
 * Летящие данные версии 0.1.0 (строки экземпляров без `wake_at`, steps —
 * массивом, сообщения в агрегате экземпляра с id без воплощения) — как их
 * оставляет 0.1.0, и `sagaUpgradeSql` после выкатки.
 */
describeDb('sagaUpgradeSql: летящие экземпляры и сообщения 0.1.0', () => {
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

  const upgrade = async (): Promise<number[]> => {
    const counts: number[] = [];
    for (const statement of sagaUpgradeSql({
      instances: TEST_TABLES.instances,
      journal: TEST_TABLES.journal,
      outbox: TEST_TABLES.outbox,
    }).split('--> statement-breakpoint')) {
      counts.push((await database.pool.query(statement)).rowCount ?? 0);
    }
    return counts;
  };

  /** Переписать сообщение так, как его записала 0.1.0: агрегат экземпляра и id без воплощения. */
  const asLegacy = async (
    eventType: string,
    sagaId: string,
    kind: 'remote' | 'timer' | 'command',
  ): Promise<void> => {
    const { rows } = await database.pool.query<{ id: string; payload: Record<string, unknown> }>(
      `SELECT id, payload FROM ${TEST_TABLES.outbox}
        WHERE event_type = $1 AND status = 'pending' AND (aggregate_id = $2 OR payload->>'sagaId' = $2
              OR payload->'sagaCommand'->>'sagaId' = $2)`,
      [eventType, sagaId],
    );
    expect(rows).toHaveLength(1);
    const row = rows[0] as { id: string; payload: Record<string, unknown> };
    const address = (kind === 'remote' ? row.payload.sagaCommand : row.payload) as SagaCommandMeta;
    const id = sagaMessageId[kind]({
      sagaId,
      generation: address.generation,
      phase: address.phase,
      stepIndex: address.stepIndex,
      attempt: address.attempt,
      incarnation: null,
    });
    const payload =
      kind === 'remote'
        ? {
            ...row.payload,
            // Ключ шага 0.1.0 — без воплощения (формат 1).
            sagaCommand: {
              ...address,
              commandId: id,
              incarnation: undefined,
              stepKey: `${sagaId}:${address.phase}:${address.stepIndex}`,
            },
          }
        : { ...row.payload, commandId: id };
    await database.pool.query(
      `UPDATE ${TEST_TABLES.outbox} SET id = $2, payload = $3::jsonb, aggregate_type = 'saga', aggregate_id = $4
        WHERE id = $1`,
      [row.id, id, JSON.stringify(payload), sagaId],
    );
  };

  it('wake_at, агрегаты таймера, отложенной команды, команды участнику и ответа; после — FIFO не держит, сага доезжает', async () => {
    const billing = newBillingScript();
    const payment = paymentSaga();
    const script = newScript();
    script.execute.set('reserve', 1);
    const order = orderSaga(script);
    // Участника у прежней реплики нет: команда ему ждёт на повторах доставки.
    const old: SagaReplica = createReplica({ pool: database.pool, clock, definitions: [payment, order] });
    // u-timer ждёт ответа участника (таймер на 30 с); u-pause — паузы повтора шага.
    await old.start(payment, 'u-timer', { orderId: '1' });
    await old.start(order, 'u-pause', { orderId: '2' });
    for (let pass = 0; pass < 6; pass += 1) {
      await old.relay.runOnce();
    }
    await asLegacy('billing.charge', 'u-timer', 'remote');
    await asLegacy('saga.timer', 'u-timer', 'timer');
    await asLegacy('saga.command', 'u-pause', 'command');
    const { rows: charge } = await database.pool.query<{ payload: { sagaCommand: SagaCommandMeta } }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'billing.charge'`,
    );
    const sent = charge[0]?.payload.sagaCommand as SagaCommandMeta;
    const legacyCommand = sent.commandId;
    // Ответ участника 0.1.0 — в агрегате экземпляра.
    await old.transaction((tx) =>
      old.outbox.enqueue(
        tx,
        [
          sagaReplyMessage(
            sagaReplyOf({ ...sent, commandId: legacyCommand }, { ok: true, data: { chargeId: 'c' } }),
          ),
        ],
        clock.now,
      ),
    );
    await database.pool.query(
      `UPDATE ${TEST_TABLES.outbox} SET aggregate_type = 'saga', aggregate_id = 'u-timer' WHERE event_type = 'saga.reply'`,
    );
    await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET steps = steps->'names', wake_at = NULL`);
    const timerDue = START.getTime() + 30_000;

    // До доводки: ждущие сроков выглядят замёрзшими.
    clock.now = new Date(START.getTime() + 2 * MINUTE);
    const stalledBefore = (await old.admin.stalledReport(MINUTE)).map((item) => item.id).sort();
    clock.now = new Date(START);

    expect(await upgrade()).toEqual([2, 0, 1, 1, 1, 1, ...SCHEMA_STATEMENTS]);
    // Идемпотентно: второй прогон ничего не меняет.
    expect(await upgrade()).toEqual([0, 0, 0, 0, 0, 0, ...SCHEMA_STATEMENTS]);

    const { rows } = await database.pool.query<{ event_type: string; aggregate_type: string; own: boolean }>(
      `SELECT event_type, aggregate_type, aggregate_id = id::text AS own FROM ${TEST_TABLES.outbox}
        WHERE status = 'pending' ORDER BY event_type`,
    );
    expect({
      stalledBefore,
      wake: [
        (await instanceOf(old, 'u-timer')).wakeAt?.getTime(),
        (await instanceOf(old, 'u-pause')).wakeAt?.getTime(),
      ],
      rows,
    }).toEqual({
      stalledBefore: ['u-pause', 'u-timer'],
      wake: [timerDue, START.getTime() + 750],
      rows: [
        { event_type: 'billing.charge', aggregate_type: 'saga-remote:1', own: false },
        { event_type: 'saga.command', aggregate_type: 'saga-deferred', own: true },
        { event_type: 'saga.reply', aggregate_type: 'saga-reply', own: true },
        { event_type: 'saga.timer', aggregate_type: 'saga-timer', own: true },
      ],
    });
    clock.now = new Date(START.getTime() + 20_000);
    expect(await old.admin.stalledReport(30_000)).toEqual([]);

    // Новая реплика с FIFO по агрегату: обе саги доезжают, ответ не ждёт таймера.
    await old.relay.stop({ deadlineMs: 1_000 });
    const fresh = createReplica({
      pool: database.pool,
      clock,
      definitions: [payment, order],
      ordering: OUTBOX_ORDERING.aggregate,
      participants: billingParticipant(billing),
    });
    await pump(clock, [fresh]);
    expect({
      timer: (await instanceOf(fresh, 'u-timer')).status,
      pause: (await instanceOf(fresh, 'u-pause')).status,
      chargeId: (await instanceOf(fresh, 'u-timer')).context.chargeId,
    }).toEqual({ timer: SAGA_STATUS.completed, pause: SAGA_STATUS.completed, chargeId: 'c' });
  });

  it('ответ применила реплика 0.1.0 (wake_at не сброшен), доставка следующего шага ушла в DEAD: до доводки не виден замёрзшим, после — виден', async () => {
    const definition = defineSaga<{ o: string }>('mixed-wake', { version: 1 })
      .remoteStep('charge', {
        command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
        timeoutMs: 60 * MINUTE,
      })
      .step('ship', { execute: async () => ({ shipped: true }) })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    replica.handlers.register('billing.charge', async () => undefined);
    // Двойник: экземпляр, честно ждущий таймера ответа, — его срок доводка сохраняет.
    await replica.start(definition, 'w-stale', { o: '1' });
    await replica.start(definition, 'w-waits', { o: '2' });
    for (let pass = 0; pass < 4; pass += 1) {
      await replica.relay.runOnce();
    }
    const timerDue = START.getTime() + 60 * MINUTE;
    expect((await instanceOf(replica, 'w-stale')).wakeAt?.getTime()).toBe(timerDue);
    // Ответ применила 0.1.0: шаг сдвинут, wake_at она не знает и не трогает;
    // команда следующего шага доставлялась и ушла в DEAD.
    await database.pool.query(
      `UPDATE ${TEST_TABLES.instances} SET step_index = 1, attempt = 1, updated_at = $1 WHERE id = 'w-stale'`,
      [new Date(START.getTime() + MINUTE)],
    );
    clock.now = new Date(START.getTime() + 21 * MINUTE);
    const before = (await replica.admin.stalledReport(10 * MINUTE)).map((item) => item.id);

    expect(await upgrade()).toEqual([0, 1, 0, 0, 0, 0, ...SCHEMA_STATEMENTS]);
    expect({
      before,
      after: (await replica.admin.stalledReport(10 * MINUTE)).map((item) => item.id),
      wakeStale: (await instanceOf(replica, 'w-stale')).wakeAt,
      wakeWaits: (await instanceOf(replica, 'w-waits')).wakeAt?.getTime(),
    }).toEqual({ before: [], after: ['w-stale'], wakeStale: null, wakeWaits: timerDue });
  });

  it('wake_at по ждущему сообщению — только у срока, который сообщение действительно несёт: немедленная команда и сообщение на повторах доставки срока не дают', async () => {
    const at = (ms: number): Date => new Date(START.getTime() + ms);
    const instance = async (id: string, wakeAt: Date | null): Promise<void> => {
      await database.pool.query(
        `INSERT INTO ${TEST_TABLES.instances}
           (id, name, version, steps, status, step_index, attempt, generation, context, created_at, updated_at, wake_at)
         VALUES ($1, 'w', 1, '["a"]', 'running', 0, $2, 0, '{}', $3, $3, $4)`,
        [id, id === 'w-pause' ? 2 : 1, START, wakeAt],
      );
    };
    const message = async (
      sagaId: string,
      eventType: string,
      attempt: number,
      attempts: number,
      nextAttemptAt: Date,
    ): Promise<void> => {
      await database.pool.query(
        `INSERT INTO ${TEST_TABLES.outbox}
           (id, aggregate_type, aggregate_id, event_type, payload, status, attempts, max_attempts, next_attempt_at, created_at)
         VALUES (gen_random_uuid(), 'saga', $1, $2, $3::jsonb, 'pending', $4, 10, $5, $6)`,
        [
          sagaId,
          eventType,
          JSON.stringify({
            commandId: sagaId,
            sagaId,
            generation: 0,
            phase: 'execute',
            stepIndex: 0,
            attempt,
          }),
          attempts,
          nextAttemptAt,
          START,
        ],
      );
    };
    // Немедленная команда, ещё не доставлявшаяся: 0.2.0 срока ей не пишет.
    await instance('w-now', null);
    await message('w-now', 'saga.command', 1, 0, START);
    // Немедленная команда на повторах доставки: срок повтора релея — не срок саги.
    await instance('w-retry', null);
    await message('w-retry', 'saga.command', 1, 3, at(10 * MINUTE));
    // Двойник: отложенная команда (пауза повтора шага), не доставлявшаяся, — её срок.
    await instance('w-pause', null);
    await message('w-pause', 'saga.command', 2, 0, at(750));
    // Двойник: таймер на повторах доставки, срок записала реплика 0.2.0 — сохранён.
    await instance('w-timer', at(30_000));
    await message('w-timer', 'saga.timer', 1, 2, at(10 * MINUTE));

    await upgrade();
    const wake = async (id: string): Promise<number | null> =>
      (
        await instanceOf(createReplica({ pool: database.pool, clock, definitions: [] }), id)
      ).wakeAt?.getTime() ?? null;
    clock.now = at(5 * MINUTE);
    const stalled = (
      await createReplica({ pool: database.pool, clock, definitions: [] }).admin.stalledReport(MINUTE)
    ).map((item) => item.id);

    expect({
      wake: {
        now: await wake('w-now'),
        retry: await wake('w-retry'),
        pause: await wake('w-pause'),
        timer: await wake('w-timer'),
      },
      stalled: stalled.sort(),
    }).toEqual({
      wake: { now: null, retry: null, pause: START.getTime() + 750, timer: START.getTime() + 30_000 },
      // Простой — от последнего сдвига: команда на повторах доставки не прячет его до срока повтора.
      stalled: ['w-now', 'w-pause', 'w-retry', 'w-timer'],
    });
  });

  it('outbox без зоны (naiveLocal): срок из стенного времени зоны писателя — исполнением, при другой зоне сессии', async () => {
    const tables = { outbox: 'naive_outbox', instances: 'naive_instances', journal: 'naive_journal' };
    for (const ddl of [
      outboxTableSql({ name: tables.outbox, timestamps: OUTBOX_TIMESTAMP.naiveLocal }),
      sagaTablesSql({ instances: tables.instances, journal: tables.journal }),
    ]) {
      for (const statement of ddl.split('--> statement-breakpoint')) {
        await database.pool.query(statement);
      }
    }
    await database.pool.query(
      `INSERT INTO ${tables.instances} (id, name, version, steps, status, step_index, attempt, generation, context, created_at, updated_at)
       VALUES ('n-1', 'pay', 1, '["a"]', 'running', 0, 1, 0, '{}', '2026-11-01T12:00:00Z', '2026-11-01T12:00:00Z')`,
    );
    // Писатель в Europe/Moscow записал срок таймера 15:30 стенным временем (= 12:30Z).
    await database.pool.query(
      `INSERT INTO ${tables.outbox} (id, aggregate_type, aggregate_id, event_type, payload, status, attempts, max_attempts, next_attempt_at, created_at)
       VALUES (gen_random_uuid(), 'saga', 'n-1', 'saga.timer',
               '{"commandId":"00000000-0000-5000-8000-000000000000","sagaId":"n-1","generation":0,"phase":"execute","stepIndex":0,"attempt":1}',
               'pending', 0, 10, '2026-11-01 15:30:00', '2026-11-01 15:00:00')`,
    );
    const [first] = sagaUpgradeSql({
      instances: tables.instances,
      journal: tables.journal,
      outbox: tables.outbox,
      outboxNaiveTimeZone: 'Europe/Moscow',
    }).split('--> statement-breakpoint');
    const client = await database.pool.connect();
    try {
      await client.query('BEGIN');
      // Зона сессии — другая: срок не зависит от неё.
      await client.query(`SET LOCAL TimeZone = 'America/New_York'`);
      await client.query(first as string);
      const { rows } = await client.query<{ wake: Date }>(`SELECT wake_at AS wake FROM ${tables.instances}`);
      await client.query('COMMIT');
      expect(rows[0]?.wake.toISOString()).toBe('2026-11-01T12:30:00.000Z');
    } finally {
      client.release();
    }
  });

  it('outbox со временем без зоны: срок — из стенного времени зоны процесса; негодная зона — отказ', () => {
    const naive = sagaUpgradeSql({
      instances: 'i',
      journal: 'j',
      outbox: 'o',
      schema: 'app',
      outboxNaiveTimeZone: 'Europe/Moscow',
      eventTypes: { timer: "my.timer'x" },
    });
    expect(naive).toContain(`max((o.next_attempt_at AT TIME ZONE 'Europe/Moscow'))`);
    expect(naive).toContain(`'my.timer''x'`);
    expect(naive).toContain('"app"."i"');
    expect(() =>
      sagaUpgradeSql({ instances: 'i', journal: 'j', outbox: 'o', outboxNaiveTimeZone: "UTC'; DROP" }),
    ).toThrow(/outboxNaiveTimeZone/);
  });
});

/**
 * Схема 0.4.0 на СУЩЕСТВУЮЩИХ таблицах 0.3.0 с данными: CHECK — NOT VALID и
 * VALIDATE, индекс по последнему движению — CONCURRENTLY; инструкции по одной,
 * вне транзакции, повторно — без отказа.
 */
describeDb('sagaUpgradeSql: схема 0.4.0 на таблицах 0.3.0', () => {
  let database: TestDatabase;
  const SCHEMA = 'saga_v030';
  const options = {
    instances: 'saga_instances',
    journal: 'saga_journal',
    outbox: 'app_outbox',
    schema: SCHEMA,
  };

  const run = async (): Promise<void> => {
    for (const statement of sagaUpgradeSql(options).split('--> statement-breakpoint')) {
      await database.pool.query(statement);
    }
  };

  const insertInstance = (id: string, status: string, attempt = 1) =>
    database.pool.query(
      `INSERT INTO ${SCHEMA}.saga_instances (id, name, version, steps, status, step_index, attempt, context, created_at, updated_at)
       VALUES ($1, 'order', 1, '["a"]', $2, 0, $3, '{}', now(), now())`,
      [id, status, attempt],
    );

  beforeEach(async () => {
    database = await createTestDatabase();
    await database.pool.query(`CREATE SCHEMA ${SCHEMA}`);
    for (const ddl of legacyV030TablesSql(SCHEMA)) {
      await database.pool.query(ddl);
    }
    await database.pool.query(
      outboxTableSql({ name: 'app_outbox', timestamps: OUTBOX_TIMESTAMP.zoned })
        .replaceAll('--> statement-breakpoint', '')
        .replaceAll('"app_outbox"', `"${SCHEMA}"."app_outbox"`),
    );
  });

  afterEach(async () => {
    await database?.drop();
  });

  it('данные 0.3.0 проходят проверку; после — ограничения и индекс действуют, повтор доводки ничего не ломает', async () => {
    await insertInstance('a-1', SAGA_STATUS.running);
    await insertInstance('a-2', SAGA_STATUS.stuck);
    await database.pool.query(
      `INSERT INTO ${SCHEMA}.saga_journal (saga_id, event, phase, generation, at) VALUES ('a-1', 'saga-started', NULL, 0, now()),
              ('a-1', 'step-completed', 'execute', 0, now())`,
    );

    await run();
    await run();

    const { rows: checks } = await database.pool.query<{ name: string; validated: boolean }>(
      `SELECT conname AS name, convalidated AS validated FROM pg_constraint
        WHERE contype = 'c' AND connamespace = $1::regnamespace ORDER BY conname`,
      [SCHEMA],
    );
    expect(checks).toEqual([
      { name: 'saga_instances_state_check', validated: true },
      { name: 'saga_journal_phase_check', validated: true },
    ]);
    const { rows: index } = await database.pool.query<{ valid: boolean; def: string }>(
      `SELECT i.indisvalid AS valid, pg_get_indexdef(i.indexrelid) AS def FROM pg_index i
         JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'saga_instances_status_move_idx'`,
    );
    expect(index).toHaveLength(1);
    expect(index[0]?.valid).toBe(true);
    expect(index[0]?.def).toContain('GREATEST(updated_at, COALESCE(wake_at, updated_at))');
    // Новый статус принят, неизвестный и невозможные числа — отказ сервера 23514.
    await expect(insertInstance('b-1', SAGA_STATUS.blocked)).resolves.toBeDefined();
    for (const [id, status, attempt] of [
      ['x-1', 'bogus', 1],
      ['x-2', SAGA_STATUS.running, 0],
    ] as const) {
      await expect(insertInstance(id, status, attempt)).rejects.toMatchObject({ code: '23514' });
    }
    await expect(
      database.pool.query(
        `INSERT INTO ${SCHEMA}.saga_journal (saga_id, event, phase, generation, at) VALUES ('a-1', 'x', 'sideways', 0, now())`,
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('строка, нарушающая ограничение, — отказ VALIDATE с 23514, а не молча принятая схема', async () => {
    await insertInstance('bad-1', 'weird');
    await expect(run()).rejects.toMatchObject({ code: '23514' });
    // Ограничение уже стоит (NOT VALID): новые строки проверяются, старая ждёт исправления.
    await expect(insertInstance('bad-2', 'weird')).rejects.toMatchObject({ code: '23514' });
    await database.pool.query(`UPDATE ${SCHEMA}.saga_instances SET status = 'resolved' WHERE id = 'bad-1'`);
    await run();
  });

  it('команда участнику без номера шага в служебной части (чужое событие) — доводка агрегатов не падает и её не трогает', async () => {
    await database.pool.query(
      `INSERT INTO ${SCHEMA}.app_outbox (aggregate_type, aggregate_id, event_type, payload, next_attempt_at)
       VALUES ('saga', 'x', 'billing.other', '{"sagaCommand": {"sagaId": "x"}}', now())`,
    );
    await run();
    const { rows } = await database.pool.query<{ type: string }>(
      `SELECT aggregate_type AS type FROM ${SCHEMA}.app_outbox`,
    );
    expect(rows).toEqual([{ type: 'saga' }]);
  });
});
