import { type SQL, sql } from 'drizzle-orm';

import { SAGA_ACTIVE_STATUSES } from '../constants/saga-active-statuses.constant';
import { START } from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  TEST_TABLES,
  type TestDatabase,
} from '../testing/saga-test-database';
import { SagaStore } from './saga-store';

/** Узлы плана (EXPLAIN FORMAT JSON) вглубь. */
const nodesOf = (plan: Record<string, unknown>): Array<Record<string, unknown>> => [
  plan,
  ...((plan.Plans as Array<Record<string, unknown>> | undefined) ?? []).flatMap(nodesOf),
];

/**
 * Запросы замёрзших (`list` с `updatedBefore`, проход сторожа `idleActive`)
 * фильтруют и упорядочивают по последнему движению — выражению, которое индекс
 * `(status, updated_at)` не обслуживает. На объёме, где завершённых много, план
 * обязан идти индексом по выражению, а не чтением всех активных с сортировкой.
 */
describeDb('SagaStore: план запросов замёрзших', () => {
  let database: TestDatabase;
  const store = new SagaStore({ instances: TEST_TABLES.instances, journal: TEST_TABLES.journal });

  beforeAll(async () => {
    database = await createTestDatabase();
    await createSagaSchema(database.pool);
    // Половина — завершённые; активных много, и почти все ДВИГАЮТСЯ (последнее
    // движение позже порога); замёрзших — каждый двадцатый активный.
    await database.pool.query(
      `INSERT INTO ${TEST_TABLES.instances} (id, name, version, steps, status, step_index, attempt, context, created_at, updated_at, wake_at)
       SELECT 'i-' || g, 'order', 1, '["a"]', CASE WHEN g % 2 = 0 THEN 'completed' ELSE 'running' END, 0, 1, '{}',
              $1::timestamptz - make_interval(secs => g),
              CASE WHEN g % 40 = 1 THEN $1::timestamptz - make_interval(secs => g) ELSE $1::timestamptz + make_interval(secs => g) END,
              CASE WHEN g % 80 = 1 THEN $1::timestamptz + make_interval(secs => 10) END
         FROM generate_series(1, 40000) AS g`,
      [START],
    );
    await database.pool.query(`ANALYZE ${TEST_TABLES.instances}`);
  }, 60_000);

  afterAll(async () => {
    await database?.drop();
  });

  /** Исполнитель, который вместо запроса хранилища отдаёт его план. */
  const explaining = () => {
    const plans: Array<Record<string, unknown>> = [];
    return {
      plans,
      db: {
        execute: async (query: SQL) => {
          const result = await database.db.execute(sql`EXPLAIN (FORMAT JSON) ${query}`);
          const [row] = result.rows as Array<{ 'QUERY PLAN': Array<{ Plan: Record<string, unknown> }> }>;
          plans.push(row?.['QUERY PLAN'][0]?.Plan ?? {});
          return { rows: [], rowCount: 0 };
        },
      },
    };
  };

  it.each([
    [
      'list({ statuses, updatedBefore })',
      (db: never) => store.list(db, { statuses: SAGA_ACTIVE_STATUSES, updatedBefore: START, limit: 100 }),
    ],
    ['idleActive (сторож)', (db: never) => store.idleActive(db, START, null, 100)],
    [
      'idleActive после курсора',
      (db: never) =>
        store.idleActive(db, START, { at: new Date(START.getTime() - 10_000_000), id: 'i-1' }, 100),
    ],
  ] as const)('%s — индекс по последнему движению', async (_name, run) => {
    const probe = explaining();
    await run(probe.db as never);
    const used = probe.plans
      .flatMap(nodesOf)
      .map((node) => node['Index Name'])
      .filter(Boolean);
    expect(used).toContain(`${TEST_TABLES.instances}_status_move_idx`);
  });
});
