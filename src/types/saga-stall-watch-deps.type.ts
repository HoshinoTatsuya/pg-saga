import type { OutboxExecutor, OutboxStore } from '@hoshinotatsuya/outbox';

import type { SagaLogger } from './saga-logger.type';
import type { SagaMetrics } from './saga-metrics.type';
import type { SagaTableNames } from './saga-table-names.type';
import type { SagaTransactionRunner } from './saga-transaction-runner.type';

/** Зависимости сторожа (`SagaStallWatch`). */
export interface SagaStallWatchDeps {
  readonly db: OutboxExecutor;
  /** Своя транзакция перевода в `blocked` (`sagaTransactionRunner(db)`): строка экземпляра — под блокировкой. */
  readonly transaction: SagaTransactionRunner;
  /** Outbox той же базы, в которую пишет оркестратор: DEAD ожидаемого сообщения ищется по его id. */
  readonly outbox: Pick<OutboxStore, 'deadLetters'>;
  readonly tables: SagaTableNames;
  /**
   * Порог `stalled`, мс: активный экземпляр не двигался дольше (после `wake_at`),
   * а DEAD его сообщения нет. Больше самого долгого шага с паузами повтора и
   * худшего отставания релея.
   */
  readonly stalledAfterMs: number;
  /**
   * С какого простоя проверять DEAD ожидаемого сообщения, мс (по умолчанию 0 —
   * любого неподвижного после `wake_at`). DEAD раньше исчерпания попыток outbox
   * не бывает: порог только сужает выборку прохода.
   */
  readonly blockAfterMs?: number;
  /** Экземпляров за проход (по умолчанию 100): выборка идёт по кругу курсором. */
  readonly batchSize?: number;
  /** Хуки `blocked` и `stalled` (остальные сторож не зовёт). */
  readonly metrics?: Pick<SagaMetrics, 'blocked' | 'stalled'>;
  readonly logger?: SagaLogger;
  readonly clock?: () => Date;
}
