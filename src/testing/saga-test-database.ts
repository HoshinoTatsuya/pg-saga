import { randomUUID } from 'node:crypto';

import { inboxTableSql, OUTBOX_TIMESTAMP, outboxTableSql } from '@hoshinotatsuya/outbox';
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import { Client, Pool } from 'pg';

import { sagaTablesSql } from '../schema/saga-tables-sql';

/** Адрес суперпользователя: `pnpm test` поднимает postgres:17 в docker. */
export const PG_TEST_URL = process.env.PG_TEST_URL;

/** Без адреса набор пропускается, и jest-assert-no-skipped валит прогон. */
export const describeDb = PG_TEST_URL ? describe : describe.skip;

/** Имена таблиц тестовой базы. */
export const TEST_TABLES = {
  outbox: 'app_outbox',
  /** Outbox со временем БЕЗ зоны (`OUTBOX_TIMESTAMP.naiveLocal`) — вторая форма таблицы. */
  naiveOutbox: 'app_outbox_naive',
  inbox: 'app_inbox',
  instances: 'saga_instances',
  journal: 'saga_journal',
} as const;

/**
 * Отдельная база набора под РОЛЬЮ ПРИЛОЖЕНИЯ — без суперправ, владельцем своей
 * базы: суперпользователь обходит права, и проверка под ним была бы зелёной
 * по неправильной причине. `adminUrl` — суперпользователь той же базы: для
 * фикстур, которым нужна чужая роль (обрыв чужого соединения).
 */
export interface TestDatabase {
  readonly url: string;
  readonly adminUrl: string;
  readonly role: string;
  readonly pool: Pool;
  readonly db: NodePgDatabase;
  /** Ещё один пул той же базы — «вторая реплика» со своими соединениями. */
  newPool(max?: number): Pool;
  drop(): Promise<void>;
}

const DDL_BREAK = '--> statement-breakpoint';

export const createTestDatabase = async (): Promise<TestDatabase> => {
  const suffix = randomUUID().replaceAll('-', '');
  const name = `saga_${suffix}`;
  const role = `saga_app_${suffix}`;
  const admin = new Client({ connectionString: PG_TEST_URL });
  await admin.connect();
  await admin.query(
    `CREATE ROLE ${role} LOGIN PASSWORD 'app' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS`,
  );
  await admin.query(`CREATE DATABASE ${name} OWNER ${role}`);
  await admin.end();
  const adminUrl = new URL(PG_TEST_URL as string);
  adminUrl.pathname = `/${name}`;
  const url = new URL(adminUrl.toString());
  url.username = role;
  url.password = 'app';
  const pools: Pool[] = [];
  const newPool = (max = 10): Pool => {
    const pool = new Pool({ connectionString: url.toString(), max });
    // Соединение, оборванное сервером в простое, не роняет процесс набора.
    pool.on('error', () => undefined);
    pools.push(pool);
    return pool;
  };
  const pool = newPool();
  const { rows } = await pool.query<{ superuser: boolean }>(
    'SELECT rolsuper AS superuser FROM pg_roles WHERE rolname = current_user',
  );
  if (rows[0]?.superuser !== false) {
    throw new Error('тестовая роль оказалась суперпользователем: проверки прав ничего не доказали бы');
  }
  return {
    url: url.toString(),
    adminUrl: adminUrl.toString(),
    role,
    pool,
    db: drizzle(pool),
    newPool,
    drop: async () => {
      await Promise.all(pools.map((item) => item.end().catch(() => undefined)));
      const cleanup = new Client({ connectionString: PG_TEST_URL });
      await cleanup.connect();
      await cleanup.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await cleanup.query(`DROP ROLE IF EXISTS ${role}`);
      await cleanup.end();
    },
  };
};

/** Таблицы outbox, inbox, саги и эффектов шагов — DDL библиотек, как в миграции потребителя. */
export const createSagaSchema = async (pool: Pool): Promise<void> => {
  for (const ddl of [
    outboxTableSql({
      name: TEST_TABLES.outbox,
      timestamps: OUTBOX_TIMESTAMP.zoned,
      // Порядок по агрегату: спеки гоняют сагу и на outbox с ordering: aggregate.
      indexNames: {
        aggregateOrder: `${TEST_TABLES.outbox}_aggregate_order`,
        fairness: `${TEST_TABLES.outbox}_fairness`,
      },
    }),
    outboxTableSql({
      name: TEST_TABLES.naiveOutbox,
      timestamps: OUTBOX_TIMESTAMP.naiveLocal,
      indexNames: { aggregateOrder: `${TEST_TABLES.naiveOutbox}_aggregate_order` },
    }),
    inboxTableSql({ name: TEST_TABLES.inbox, timestamps: OUTBOX_TIMESTAMP.zoned }),
    sagaTablesSql({ instances: TEST_TABLES.instances, journal: TEST_TABLES.journal }),
  ]) {
    await pool.query(ddl.replaceAll(DDL_BREAK, ''));
  }
  await pool.query(
    `CREATE TABLE effects (
       id serial PRIMARY KEY, saga_id text NOT NULL, step text NOT NULL, phase text NOT NULL, attempt int NOT NULL
     )`,
  );
};

/** Очистить всё между проверками. */
export const truncateSagaSchema = async (pool: Pool): Promise<void> => {
  await pool.query(
    `TRUNCATE ${TEST_TABLES.outbox}, ${TEST_TABLES.naiveOutbox}, ${TEST_TABLES.inbox}, ${TEST_TABLES.journal}, ${TEST_TABLES.instances}, effects`,
  );
};

/** Отказ промиса → сам отказ; успех — ошибка проверки. */
export const rejectionOf = async (work: Promise<unknown>): Promise<unknown> =>
  work.then(
    () => {
      throw new Error('ожидался отказ');
    },
    (error: unknown) => error,
  );

/**
 * Ждёт, пока `count` запросов с `fragment` встанут в ожидание блокировки.
 * Опрос базы, а не таймер: проверка продолжается, когда гонка УЖЕ выстроена.
 */
export const waitForLockWaiters = async (pool: Pool, fragment: string, count: number): Promise<void> => {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_stat_activity
        WHERE datname = current_database() AND wait_event_type = 'Lock' AND query ILIKE $1`,
      [`%${fragment}%`],
    );
    if (rows[0]?.n === count) {
      return;
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 25);
    });
  }
  throw new Error(`не дождались ${count} запросов «${fragment}» в ожидании блокировки`);
};
