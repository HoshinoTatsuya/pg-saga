import type { SagaStatus } from './saga-status.type';

/**
 * Хук ручного разбора: вызывается ПОСЛЕ фиксации (откатившееся действие не
 * считается); отказ хука проглатывается — наблюдение не ломает разбор.
 */
export interface SagaAdminMetrics {
  manualRetry?(event: {
    readonly saga: string;
    readonly sagaId: string;
    /** Состояние до повтора: `stuck`, `blocked` или активное (замёрзший, `stalledForMs`). */
    readonly from: SagaStatus;
    /** Повтор замёрзшего активного (`stalledForMs`), а не застрявшего или вставшего. */
    readonly stalled: boolean;
    readonly contextPatched: boolean;
  }): void;
  manualResolve?(event: { readonly saga: string; readonly sagaId: string }): void;
}
