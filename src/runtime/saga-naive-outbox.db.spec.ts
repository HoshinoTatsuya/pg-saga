import { OUTBOX_ORDERING, type OutboxDeliveryPort } from '@hoshinotatsuya/outbox';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { Pool } from 'pg';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { createReplica, instanceOf, journalOf, START, type TestClock } from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import { sagaAggregateAheadMs } from './saga-aggregate-ahead-ms';

const MINUTE = 60_000;

/**
 * Порядок команд участнику при расхождении часов (`sagaAggregateAheadMs`) — на
 * ОБЕИХ формах outbox: со временем с зоной и без зоны (`naiveLocal`, стенное
 * время зоны процесса). Зона сессии отличается от зоны процесса: разность,
 * посчитанная в зоне сессии, ушла бы на разницу зон (часы), и возврат обогнал
 * бы списание.
 */
describeDb.each([
  ['время с зоной', TEST_TABLES.outbox],
  ['время без зоны (naiveLocal)', TEST_TABLES.naiveOutbox],
])('outbox %s: опережение агрегата и порядок команд участнику', (_label, outboxTable) => {
  let database: TestDatabase;
  let pool: Pool;
  let clock: TestClock;
  // Зона сессии — заведомо не зона процесса (в jest зону процесса не сменить).
  const sessionZone = new Date(START).getTimezoneOffset() === 300 ? 'Asia/Tokyo' : 'America/New_York';

  beforeAll(async () => {
    database = await createTestDatabase();
    await createSagaSchema(database.pool);
    const { rows } = await database.pool.query<{ name: string }>('SELECT current_database() AS name');
    await database.pool.query(`ALTER DATABASE "${rows[0]?.name}" SET TimeZone = '${sessionZone}'`);
    // Новый пул — новые сессии: зона базы применяется при подключении.
    pool = database.newPool();
  });

  afterAll(async () => {
    await database?.drop();
  });

  beforeEach(async () => {
    await truncateSagaSchema(pool);
    clock = { now: new Date(START) };
  });

  it('зоны процесса и сессии в наборе действительно разные (иначе проверка ничего не доказала бы)', async () => {
    const { rows } = await pool.query<{ zone: string; offset: number }>(
      `SELECT current_setting('TimeZone') AS zone,
              (EXTRACT(EPOCH FROM ($1::timestamptz AT TIME ZONE 'UTC') - ($1::timestamptz AT TIME ZONE current_setting('TimeZone'))) / 60)::int AS offset`,
      [START],
    );
    // Под пулером зона базы до уже открытых серверных соединений не доходит:
    // важна не она, а то, что зона сессии — не зона процесса.
    expect(rows[0]?.offset).not.toBe(new Date(START).getTimezoneOffset());
  });

  it('sagaAggregateAheadMs: ждущее событие записано на 5 мин позже своих часов — ровно 300000 мс', async () => {
    const replica = createReplica({ pool, clock, definitions: [], outboxTable });
    const ahead = new Date(START.getTime() + 5 * MINUTE);
    await replica.transaction((tx) =>
      replica.outbox.enqueue(
        tx,
        [{ aggregateType: 'billing', aggregateId: 'a-1', eventType: 'billing.charge', payload: {} }],
        ahead,
      ),
    );
    const db = drizzle(pool);
    expect({
      ahead: await sagaAggregateAheadMs(
        db,
        { table: outboxTable, schema: null },
        { type: 'billing', id: 'a-1' },
        START,
      ),
      // Двойник: чужой агрегат — ждущих нет.
      other: await sagaAggregateAheadMs(
        db,
        { table: outboxTable, schema: null },
        { type: 'billing', id: 'a-2' },
        START,
      ),
    }).toEqual({ ahead: 5 * MINUTE, other: null });
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('списание с реплики, чьи часы впереди на 5 мин: возврат с верными часами встаёт ПОСЛЕ него', async () => {
    const definition = defineSaga<{ orderId: string }>('naive-skew', { version: 1 })
      .remoteStep('charge', {
        command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
        compensation: () => ({ eventType: 'billing.refund', payload: {} }),
        timeoutMs: 3_600_000,
        retry: { maxAttempts: 1 },
      })
      .build();
    const phases: string[] = [];
    let failures = 3;
    const port: OutboxDeliveryPort = async (delivery) => {
      const { meta } = parseSagaCommand(delivery.payload);
      if (meta.phase === 'execute' && failures > 0) {
        failures -= 1;
        throw new Error('участник лежит');
      }
      phases.push(meta.phase);
    };
    const participants = (): ReadonlyArray<readonly [string, OutboxDeliveryPort]> => [
      ['billing.charge', port],
      ['billing.refund', port],
    ];
    const fast = createReplica({
      pool,
      clock: { now: new Date(START.getTime() + 5 * MINUTE) },
      definitions: [definition],
      ordering: OUTBOX_ORDERING.aggregate,
      participants,
      outboxTable,
    });
    const slow = createReplica({
      pool,
      clock,
      definitions: [definition],
      ordering: OUTBOX_ORDERING.aggregate,
      participants,
      outboxTable,
    });
    await slow.start(definition, 'n-skew', { orderId: '1' });
    // Команду списания пишет реплика с часами впереди.
    await fast.relay.runOnce();
    expect(await journalOf(slow, 'n-skew')).toContain('step-dispatched:charge');
    await slow.transaction((tx) => slow.orchestrator.cancel(tx, 'n-skew', 'клиент передумал'));
    for (let step = 0; step < 40; step += 1) {
      for (let pass = 0; pass < 4; pass += 1) {
        await slow.relay.runOnce();
      }
      clock.now = new Date(clock.now.getTime() + 15_000);
    }
    const { rows } = await pool.query<{ event_type: string; ahead_ms: number }>(
      `SELECT event_type, (EXTRACT(EPOCH FROM (created_at - min(created_at) OVER ())) * 1000)::float8 AS ahead_ms
         FROM ${outboxTable} WHERE event_type IN ('billing.charge', 'billing.refund') ORDER BY created_at, id`,
    );
    // Возврат записан строго позже списания (на 1 мс), хотя свои часы реплики были на 5 мин позади.
    expect(rows).toEqual([
      { event_type: 'billing.charge', ahead_ms: 0 },
      { event_type: 'billing.refund', ahead_ms: 1 },
    ]);
    expect({ phases, failures, status: (await instanceOf(slow, 'n-skew')).status }).toEqual({
      phases: ['execute', 'compensate'],
      failures: 0,
      status: SAGA_STATUS.compensating,
    });
    await fast.relay.stop({ deadlineMs: 1_000 });
    await slow.relay.stop({ deadlineMs: 1_000 });
  });
});
