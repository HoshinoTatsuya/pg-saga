import type { SagaPhase } from './saga-phase.type';

/** Следующая команда исполнителю саги (событие `saga.command`). */
export interface SagaCommandDraft {
  readonly phase: SagaPhase;
  readonly stepIndex: number;
  readonly attempt: number;
  readonly generation: number;
  /**
   * Пауза до доставки, мс (повтор); `null` — сразу. Относительная: срок
   * считает оркестратор от СВОИХ часов, а не от отметки перехода.
   */
  readonly delayMs: number | null;
}
