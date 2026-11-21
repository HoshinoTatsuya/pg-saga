import type { OutboxExecutor, OutboxInbox, OutboxStore } from '@hoshinotatsuya/outbox';

import type { SagaEventTypes } from './saga-event-types.type';
import type { SagaLogger } from './saga-logger.type';
import type { SagaMetrics } from './saga-metrics.type';
import type { SagaTableNames } from './saga-table-names.type';
import type { SagaTransactionRunner } from './saga-transaction-runner.type';

/** Зависимости оркестратора. */
export interface SagaOrchestratorDeps {
  /** База drizzle над пулом (`drizzle(pool)`): чтение и, без `transaction`, свои транзакции. */
  readonly db: OutboxExecutor;
  /**
   * Хранилище outbox той же базы: команды, таймеры и ответы пишутся в него. Его
   * `config.deliveryTimeoutMs` — потолок доставки релея, построенного над ним:
   * с ним сверяются локальные шаги; правила слияния (`config.coalescing`) —
   * не для типов событий саги.
   */
  readonly outbox: Pick<OutboxStore, 'enqueue' | 'config'>;
  /** Inbox той же базы: каждое сообщение саги применяется один раз. */
  readonly inbox: Pick<OutboxInbox, 'once'>;
  readonly tables: SagaTableNames;
  /** Своя транзакция; по умолчанию — `sagaTransactionRunner(db, { lockTimeoutMs })`. */
  readonly transaction?: SagaTransactionRunner;
  /** Потолок ожидания блокировок в своей транзакции, мс (без `transaction`). */
  readonly lockTimeoutMs?: number;
  /** Получатель inbox; по умолчанию `saga`. */
  readonly consumer?: string;
  readonly eventTypes?: Partial<SagaEventTypes>;
  readonly metrics?: SagaMetrics;
  readonly logger?: SagaLogger;
  /** Часы процесса; по умолчанию `new Date()`. */
  readonly clock?: () => Date;
  /** Источник случайной части паузы; по умолчанию `Math.random`. */
  readonly random?: () => number;
}
