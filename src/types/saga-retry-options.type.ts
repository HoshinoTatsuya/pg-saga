/**
 * Повтор шага или компенсации. Пауза перед попыткой N+1 —
 * `min(maxBackoffMs, backoffMs × 2^(N−1))`, из них половина — случайная
 * (реплики, отказавшие вместе, не повторяют в ногу).
 */
export interface SagaRetryOptions {
  /** Попыток всего, включая первую; целое ≥ 1. */
  readonly maxAttempts?: number;
  readonly backoffMs?: number;
  readonly maxBackoffMs?: number;
}
