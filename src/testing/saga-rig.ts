import {
  type OutboxDeliveryPort,
  type OutboxFairness,
  OutboxHandlers,
  OutboxInbox,
  type OutboxLogger,
  type OutboxOrdering,
  OutboxRelay,
  OutboxStore,
  type OutboxTransaction,
} from '@hoshinotatsuya/outbox';
import { sql } from 'drizzle-orm';
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import type { Pool } from 'pg';

import { SagaAdmin } from '../admin/saga-admin';
import { SagaOrchestrator } from '../runtime/saga-orchestrator';
import { sagaTransactionRunner } from '../runtime/saga-transaction-runner';
import type { SagaDefinition } from '../types/saga-definition.type';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaMetrics } from '../types/saga-metrics.type';
import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';
import { TEST_TABLES } from './saga-test-database';

/** Общие часы набора: релей и оркестратор видят одно и то же «сейчас». */
export interface TestClock {
  now: Date;
}

export const START = new Date('2026-11-01T12:00:00.000Z');

/**
 * Одна «реплика»: свой пул, outbox, inbox, оркестратор, релей и обработчики.
 * Часы — общие на набор и двигаются только `pump` (к сроку следующего события).
 */
export interface SagaReplica {
  readonly clock: TestClock;
  readonly pool: Pool;
  readonly db: NodePgDatabase;
  readonly outbox: OutboxStore;
  readonly inbox: OutboxInbox;
  readonly orchestrator: SagaOrchestrator;
  readonly admin: SagaAdmin;
  readonly handlers: OutboxHandlers;
  readonly relay: OutboxRelay;
  readonly transaction: SagaTransactionRunner;
  /** Предупреждения релея: отказ доставки виден здесь, а не пропадает. */
  readonly warnings: string[];
  /** Предупреждения оркестратора (`deps.logger`). */
  readonly sagaWarnings: string[];
  /** Запустить экземпляр в своей транзакции. */
  start(definition: SagaDefinition<object, object>, id: string, input: object): Promise<boolean>;
}

export interface ReplicaOptions {
  readonly pool: Pool;
  readonly clock: TestClock;
  readonly definitions: ReadonlyArray<SagaDefinition<object, object>>;
  readonly transaction?: (inner: SagaTransactionRunner) => SagaTransactionRunner;
  readonly metrics?: SagaMetrics;
  readonly random?: () => number;
  /** Порядок доставки outbox (`OUTBOX_ORDERING`): сага обязана работать и с FIFO по агрегату. */
  readonly ordering?: OutboxOrdering;
  /** Справедливый захват outbox (`OUTBOX_FAIRNESS`): порядок агрегатов саги обязан держаться и с ним. */
  readonly fairness?: OutboxFairness;
  /** Таблица outbox (по умолчанию `TEST_TABLES.outbox`, время с зоной). */
  readonly outboxTable?: string;
  /** Попытки и срок жизни событий outbox (по умолчанию — outbox). */
  readonly outboxLimits?: { readonly maxAttempts?: number; readonly deadAfterMs?: number };
  /** Участники удалённых шагов: регистрируются в обработчиках этой реплики. */
  readonly participants?: (replica: SagaReplica) => ReadonlyArray<readonly [string, OutboxDeliveryPort]>;
}

export const createReplica = (options: ReplicaOptions): SagaReplica => {
  const { pool, clock } = options;
  const db = drizzle(pool);
  const outbox = new OutboxStore(db, {
    table: options.outboxTable ?? TEST_TABLES.outbox,
    backoffBaseMs: 1_000,
    backoffMaxMs: 5_000,
    ...options.outboxLimits,
    ...(options.ordering === undefined ? {} : { ordering: options.ordering }),
    ...(options.fairness === undefined ? {} : { fairness: options.fairness }),
  });
  const inbox = new OutboxInbox(db, { table: TEST_TABLES.inbox });
  const base = sagaTransactionRunner(db, { lockTimeoutMs: 5_000 });
  const transaction = options.transaction?.(base) ?? base;
  const tables = { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal };
  const sagaWarnings: string[] = [];
  const orchestrator = new SagaOrchestrator({
    db,
    outbox,
    inbox,
    tables,
    transaction,
    clock: () => clock.now,
    random: options.random ?? ((): number => 0.5),
    ...(options.metrics === undefined ? {} : { metrics: options.metrics }),
    logger: { warn: (message) => sagaWarnings.push(message) },
  });
  for (const definition of options.definitions) {
    orchestrator.register(definition);
  }
  const handlers = new OutboxHandlers();
  orchestrator.registerHandlers(handlers);
  const warnings: string[] = [];
  const logger: OutboxLogger = {
    log: () => undefined,
    warn: (message) => warnings.push(message),
    error: (message) => warnings.push(message),
  };
  const relay = new OutboxRelay({ store: outbox, deliver: handlers.deliver, clock: () => clock.now, logger });
  const admin = new SagaAdmin({
    db,
    transaction,
    outbox,
    tables,
    commandEventType: orchestrator.eventTypes.command,
    clock: () => clock.now,
  });
  const replica: SagaReplica = {
    clock,
    pool,
    db,
    outbox,
    inbox,
    orchestrator,
    admin,
    handlers,
    relay,
    transaction,
    warnings,
    sagaWarnings,
    start: async (definition, id, input) =>
      (
        await base((tx) =>
          orchestrator.start(tx, definition as SagaDefinition, {
            id,
            input: input as Record<string, unknown>,
          }),
        )
      ).started,
  };
  for (const [eventType, port] of options.participants?.(replica) ?? []) {
    handlers.register(eventType, (_payload, delivery, context) => port(delivery, context));
  }
  return replica;
};

/**
 * Прогнать релей(и) до тишины: проход; пусто — часы к сроку ближайшего
 * PENDING-события (пауза повтора, таймер шага); событий нет — готово.
 * Реплики идут ОДНОВРЕМЕННО — гонка за одни и те же события настоящая.
 */
export const pump = async (
  clock: TestClock,
  replicas: readonly SagaReplica[],
  maxRounds = 400,
): Promise<{ rounds: number }> => {
  const [first] = replicas;
  if (first === undefined) {
    throw new Error('pump: нет реплик');
  }
  for (let round = 1; round <= maxRounds; round += 1) {
    const reports = await Promise.all(replicas.map((replica) => replica.relay.runOnce()));
    if (reports.some((report) => report.claimed > 0)) {
      continue;
    }
    const { rows } = await first.pool.query<{ next: Date | null; claimed: Date | null }>(
      `SELECT min(next_attempt_at) FILTER (WHERE status = 'pending') AS next,
              min(claimed_at) FILTER (WHERE status = 'dispatching') AS claimed
         FROM ${TEST_TABLES.outbox}`,
    );
    // Захват, брошенный умершим процессом, вернёт перехват — после окна перехвата.
    const claimed = rows[0]?.claimed ?? null;
    const reclaimAt =
      claimed === null ? null : new Date(claimed.getTime() + first.outbox.config.reclaimStuckAfterMs + 1);
    const candidates = [rows[0]?.next ?? null, reclaimAt].filter((value): value is Date => value !== null);
    if (candidates.length === 0) {
      return { rounds: round };
    }
    const next = new Date(Math.min(...candidates.map((value) => value.getTime())));
    if (next.getTime() > clock.now.getTime()) {
      clock.now = next;
    }
  }
  throw new Error(`pump: не затихло за ${maxRounds} проходов`);
};

/** Экземпляр по id (для проверок) — прямо из таблицы. */
export const instanceOf = async (replica: SagaReplica, id: string): Promise<SagaInstance> => {
  const shown = await replica.admin.show(id);
  if (shown === null) {
    throw new Error(`экземпляра ${id} нет`);
  }
  return shown.instance;
};

/** Эффект шага в транзакции шага. */
export const recordEffect = async (
  tx: OutboxTransaction,
  sagaId: string,
  step: string,
  phase: string,
  attempt: number,
): Promise<void> => {
  await tx.execute(
    sql`INSERT INTO effects (saga_id, step, phase, attempt) VALUES (${sagaId}, ${step}, ${phase}, ${attempt})`,
  );
};

/** Эффекты экземпляра по порядку записи: `шаг:фаза`. */
export const effectsOf = async (pool: Pool, sagaId: string): Promise<string[]> =>
  (
    await pool.query<{ step: string; phase: string }>(
      'SELECT step, phase FROM effects WHERE saga_id = $1 ORDER BY id',
      [sagaId],
    )
  ).rows.map((row) => `${row.step}:${row.phase}`);

/** События журнала экземпляра по порядку: `событие[:шаг]`. */
export const journalOf = async (replica: SagaReplica, sagaId: string): Promise<string[]> =>
  ((await replica.admin.show(sagaId))?.journal ?? []).map((entry) =>
    entry.step === null ? entry.event : `${entry.event}:${entry.step}`,
  );
