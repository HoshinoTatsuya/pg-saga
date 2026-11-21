import type { SagaPhase } from './saga-phase.type';

/**
 * Служебная часть команды удалённому участнику (`sagaCommand` в payload):
 * всё, что нужно, чтобы ответить (`sagaReplyOf`) и не выполнить шаг дважды
 * (`stepKey` — один во всех попытках).
 */
export interface SagaCommandMeta {
  readonly commandId: string;
  readonly sagaId: string;
  readonly saga: string;
  readonly version: number;
  readonly generation: number;
  readonly phase: SagaPhase;
  readonly stepIndex: number;
  readonly step: string;
  readonly attempt: number;
  readonly stepKey: string;
  /** Воплощение экземпляра — у ключа формата 2; нет — экземпляр версии 0.1.0 (формат 1). */
  readonly incarnation?: number;
  /** Ключ выполнения шага — в команде компенсации (`meta.phase === 'compensate'`). */
  readonly executeStepKey?: string;
}
