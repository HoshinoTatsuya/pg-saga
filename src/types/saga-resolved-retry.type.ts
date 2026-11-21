/** Повтор после подстановки значений по умолчанию. */
export interface SagaResolvedRetry {
  readonly maxAttempts: number;
  readonly backoffMs: number;
  readonly maxBackoffMs: number;
}
