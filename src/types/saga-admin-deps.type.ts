import type { OutboxExecutor, OutboxStore } from '@hoshinotatsuya/outbox';
import type { SagaAdminMetrics } from './saga-admin-metrics.type';

import type { SagaTableNames } from './saga-table-names.type';
import type { SagaTransactionRunner } from './saga-transaction-runner.type';

/** Зависимости ручного разбора. */
export interface SagaAdminDeps {
  readonly db: OutboxExecutor;
  /** Своя транзакция: `sagaTransactionRunner(db)` или `withPgTransaction` из `@hoshinotatsuya/outbox/pg`. */
  readonly transaction: SagaTransactionRunner;
  /** Outbox той же базы: ручной повтор пишет в него команду компенсации. */
  readonly outbox?: Pick<OutboxStore, 'enqueue'>;
  readonly tables: SagaTableNames;
  /**
   * Тип команды — тот же, что у оркестратора (`orchestrator.eventTypes.command`).
   * Нужен `retry` (без него — отказ до записи); прочим операциям не нужен.
   */
  readonly commandEventType?: string;
  readonly clock?: () => Date;
  /** Хуки ручных действий (после фиксации). */
  readonly metrics?: SagaAdminMetrics;
}
