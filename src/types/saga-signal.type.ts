import type { SagaMetrics } from './saga-metrics.type';

/** Вызов хука метрик, отложенный до фиксации транзакции перехода. */
export type SagaSignal = {
  [K in keyof SagaMetrics]-?: {
    readonly kind: K;
    readonly event: Parameters<NonNullable<SagaMetrics[K]>>[0];
  };
}[keyof SagaMetrics];
