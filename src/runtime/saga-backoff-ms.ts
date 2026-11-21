import type { SagaResolvedRetry } from '../types/saga-resolved-retry.type';

/**
 * Пауза перед попыткой `attempt + 1`: `min(крышка, база × 2^(attempt−1))`,
 * половина — постоянная, половина — случайная. Без случайной части реплики,
 * отказавшие на одном и том же (база недоступна), повторяли бы в ногу.
 */
export const sagaBackoffMs = (retry: SagaResolvedRetry, attempt: number, random: () => number): number => {
  const full = Math.min(retry.maxBackoffMs, retry.backoffMs * 2 ** Math.max(0, attempt - 1));
  const half = full / 2;
  // Источник, отдавший не число (NaN, Infinity), — середина: иначе NaN в сроке
  // команды (Invalid Date) и отказ записи на каждой доставке до DEAD.
  const value = random();
  const share = Number.isFinite(value) ? Math.min(1, Math.max(0, value)) : 0.5;
  return Math.round(half + half * share);
};
