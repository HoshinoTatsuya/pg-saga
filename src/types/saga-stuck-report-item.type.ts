import type { SagaStatus } from './saga-status.type';

/** Застрявший экземпляр в отчёте (`SagaAdmin.stuckReport`). */
export interface SagaStuckReportItem {
  readonly id: string;
  readonly saga: string;
  readonly version: number;
  /** Шаг, чья компенсация исчерпала повторы. */
  /** `stuck` — в отчёте о застрявших; `running`/`compensating` — в отчёте о замёрзших. */
  readonly status: SagaStatus;
  readonly step: string;
  readonly attempt: number;
  readonly failure: string | null;
  readonly lastError: string | null;
  readonly stuckForMs: number;
}
