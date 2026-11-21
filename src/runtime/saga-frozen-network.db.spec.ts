import { setTimeout as sleep } from 'node:timers/promises';

import { FaultProxy } from '@hoshinotatsuya/fault-kit/tcp';
import { OutboxInbox, OutboxRelay, OutboxStore } from '@hoshinotatsuya/outbox';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import { SagaAdmin } from '../admin/saga-admin';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { START, type TestClock } from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import { SagaOrchestrator } from './saga-orchestrator';
import { sagaTransactionRunner } from './saga-transaction-runner';

/** Потолок доставки релея в наборе — он же потолок простоя транзакции оркестратора. */
const DELIVERY_MS = 5_000;
const LOCK_MS = 1_000;
const TABLES = { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal };

/**
 * Замёрзшая сеть реплики посреди шага (FaultProxy: байты не идут, сокеты
 * открыты). Клиент отпускает соединение по своим таймаутам, а сервер этого не
 * видит: сессия стоит «idle in transaction» с блокировкой строки экземпляра и
 * ключом inbox. Отпускает её только потолок простоя транзакции, который
 * оркестратор ставит равным потолку доставки релея, — и он же не обрывает
 * ЗАКОННЫЙ шаг, ждущий между своими запросами.
 */
describeDb('SagaOrchestrator: замёрзшая сеть и простой транзакции', () => {
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

  /** Оркестратор со СВОЕЙ транзакцией (без `deps.transaction`) и релей над пулом. */
  const replicaOn = (pool: Pool, definitions: ReturnType<typeof holdSaga>[]) => {
    const db = drizzle(pool);
    const outbox = new OutboxStore(db, { table: TEST_TABLES.outbox, deliveryTimeoutMs: DELIVERY_MS });
    const orchestrator = new SagaOrchestrator({
      db,
      outbox,
      inbox: new OutboxInbox(db, { table: TEST_TABLES.inbox }),
      tables: TABLES,
      lockTimeoutMs: LOCK_MS,
      clock: () => clock.now,
      logger: { warn: () => undefined },
    });
    for (const definition of definitions) {
      orchestrator.register(definition);
    }
    const relay = new OutboxRelay({
      store: outbox,
      deliver: async (delivery) => {
        await orchestrator.handle(delivery.eventType, delivery.payload);
      },
      clock: () => clock.now,
      logger: { log: () => undefined, warn: () => undefined, error: () => undefined },
    });
    const admin = new SagaAdmin({ db, transaction: sagaTransactionRunner(db), tables: TABLES });
    return { db, outbox, orchestrator, relay, admin };
  };

  /** Шаг: запрос, затем ожидание вне базы `idleMs` (простой транзакции), затем запрос. */
  const holdSaga = (name: string, idleMs: number, timeoutMs: number, entered: () => void = () => undefined) =>
    defineSaga<{ orderId: string }>(name, { version: 1 })
      .step('hold', {
        execute: async ({ tx, signal }) => {
          await tx.execute(sql`SELECT 1`);
          entered();
          await sleep(idleMs, undefined, { signal }).catch(() => undefined);
          await tx.execute(sql`SELECT 2`);
          return { held: true };
        },
        timeoutMs,
        retry: { maxAttempts: 1 },
      })
      .build();

  const startOn = async (
    replica: ReturnType<typeof replicaOn>,
    definition: ReturnType<typeof holdSaga>,
    id: string,
  ) =>
    sagaTransactionRunner(replica.db)((tx) =>
      replica.orchestrator.start(tx, definition, { id, input: { orderId: '1' } }),
    );

  it('сеть реплики замёрзла посреди шага: строка экземпляра отпущена по простою, отмена проходит, пока сеть стоит', async () => {
    const url = new URL(database.url);
    const proxy = await FaultProxy.start({ host: url.hostname, port: Number(url.port) });
    url.hostname = proxy.host;
    url.port = String(proxy.port);
    const frozenPool = new Pool({ connectionString: url.toString(), max: 4 });
    frozenPool.on('error', () => undefined);
    let entered: () => void = () => undefined;
    const inStep = new Promise<void>((resolve) => {
      entered = resolve;
    });
    // Шаг дольше своего потолка: клиент бросит его по таймауту, но ROLLBACK
    // через замёрзшую сеть до сервера не дойдёт.
    const definition = holdSaga('frozen', 60_000, 1_500, () => entered());
    const healthy = replicaOn(database.pool, [definition]);
    const flaky = replicaOn(frozenPool, [definition]);
    try {
      await startOn(healthy, definition, 'z-1');
      const delivery = flaky.relay.runOnce().catch(() => undefined);
      await inStep;
      proxy.freeze();
      const passedBeforeFreeze = proxy.stats().bytesToUpstream + proxy.stats().bytesToClient;
      // Отмена из приложения — в своей транзакции с ожиданием блокировки 1 с;
      // попыток с запасом на потолок простоя (5 с) и потолок ROLLBACK клиента.
      const attempts: string[] = [];
      for (let attempt = 0; attempt < 10 && !attempts.at(-1)?.startsWith('cancel'); attempt += 1) {
        attempts.push(
          await sagaTransactionRunner(healthy.db, { lockTimeoutMs: LOCK_MS })((tx) =>
            healthy.orchestrator.cancel(tx, 'z-1', 'клиент передумал'),
          ).then(
            (result) => `cancel ${result.status}`,
            (error: { code?: string; cause?: { code?: string } }) =>
              `fail ${error.code ?? error.cause?.code ?? '?'}`,
          ),
        );
      }
      const frozenSessions = await database.pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM pg_stat_activity
          WHERE datname = current_database() AND state LIKE 'idle in transaction%'`,
      );
      // Сеть всё ещё стоит: отмену отпустил сервер, а не оттаявший клиент.
      expect({
        cancelled: attempts.at(-1),
        bytesSinceFreeze: proxy.stats().bytesToUpstream + proxy.stats().bytesToClient - passedBeforeFreeze,
        idleInTransaction: frozenSessions.rows[0]?.n,
      }).toEqual({
        cancelled: `cancel ${SAGA_STATUS.compensated}`,
        bytesSinceFreeze: 0,
        idleInTransaction: 0,
      });
      // Выборка не выродилась: строку держали — первые попытки отказали ожиданием блокировки.
      expect(attempts[0]).toBe('fail 55P03');
      proxy.thaw();
      const limit = new AbortController();
      await Promise.race([
        delivery,
        sleep(20_000, undefined, { signal: limit.signal }).catch(() => undefined),
      ]);
      limit.abort();
      expect((await healthy.admin.show('z-1'))?.instance.status).toBe(SAGA_STATUS.compensated);
    } finally {
      proxy.thaw();
      // Клиент, застрявший в доставке, может не вернуться в пул: уборка — с потолком.
      const ended = new AbortController();
      await Promise.race([
        frozenPool.end().catch(() => undefined),
        sleep(3_000, undefined, { signal: ended.signal }).catch(() => undefined),
      ]);
      ended.abort();
      await proxy.close();
    }
  }, 60_000);

  it('двойник: законный шаг, ждущий между своими запросами дольше ожидания блокировки, — не обрывается', async () => {
    // Простой 1,5 с при потолке шага 1,8 с, потолке простоя (= доставки) 5 с.
    const definition = holdSaga('patient', 1_500, 1_800);
    const replica = replicaOn(database.pool, [definition]);
    await startOn(replica, definition, 'p-1');
    await replica.relay.runOnce();
    const instance = await replica.admin.show('p-1');
    expect({ status: instance?.instance.status, held: instance?.instance.context.held }).toEqual({
      status: SAGA_STATUS.completed,
      held: true,
    });
  }, 30_000);

  it('register: локальный шаг не короче потолка простоя транзакции своего раннера — отказ на старте', () => {
    const db = drizzle(database.pool);
    const orchestrator = new SagaOrchestrator({
      db,
      outbox: new OutboxStore(db, { table: TEST_TABLES.outbox, deliveryTimeoutMs: 60_000 }),
      inbox: new OutboxInbox(db, { table: TEST_TABLES.inbox }),
      tables: TABLES,
      transaction: sagaTransactionRunner(db, { idleInTransactionTimeoutMs: 20_000 }),
    });
    expect(() => orchestrator.register(holdSaga('long', 0, 30_000))).toThrow(SagaConfigError);
    // Двойник: шаг короче потолка простоя — принят.
    expect(() => orchestrator.register(holdSaga('short', 0, 15_000))).not.toThrow();
  });
});
