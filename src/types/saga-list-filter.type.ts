import type { SagaStatus } from './saga-status.type';

/** Фильтр списка экземпляров (`SagaAdmin.list`); порядок — по последнему движению, старейшие первыми. */
export interface SagaListFilter {
  readonly status?: SagaStatus;
  /** Любой из статусов (вместе со `status` — пересечение). */
  readonly statuses?: readonly SagaStatus[];
  /**
   * Не двигались с этого момента: последнее движение — позже из `updated_at`
   * и `wake_at` (срок, до которого экземпляр штатно ждёт паузы повтора или
   * таймера ответа) — раньше `updatedBefore`.
   */
  readonly updatedBefore?: Date;
  readonly saga?: string;
  /** Не больше; по умолчанию 100. */
  readonly limit?: number;
}
