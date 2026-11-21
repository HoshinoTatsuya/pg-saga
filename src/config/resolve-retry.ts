import type { SagaResolvedRetry } from '../types/saga-resolved-retry.type';
import type { SagaRetryOptions } from '../types/saga-retry-options.type';
import { integer } from './config-integer';

/** Повтор с подстановкой значений по умолчанию; крышка паузы не меньше базы. */
export const resolveRetry = (
  field: string,
  options: SagaRetryOptions | undefined,
  defaults: SagaResolvedRetry,
): SagaResolvedRetry => {
  const maxAttempts = integer(`${field}.maxAttempts`, options?.maxAttempts ?? defaults.maxAttempts, 1);
  const backoffMs = integer(`${field}.backoffMs`, options?.backoffMs ?? defaults.backoffMs, 0);
  const maxBackoffMs = integer(
    `${field}.maxBackoffMs`,
    options?.maxBackoffMs ?? Math.max(defaults.maxBackoffMs, backoffMs),
    backoffMs,
  );
  return { maxAttempts, backoffMs, maxBackoffMs };
};
