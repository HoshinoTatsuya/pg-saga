import type { SagaPhase } from './saga-phase.type';
import type { SagaReplyOutcome } from './saga-reply-outcome.type';

/**
 * Ответ участника на команду. Применяется не больше одного раза на команду
 * (`commandId` — ключ inbox): второй ответ на ту же команду — дубль.
 */
export interface SagaReply {
  readonly commandId: string;
  readonly sagaId: string;
  readonly generation: number;
  readonly phase: SagaPhase;
  readonly stepIndex: number;
  readonly attempt: number;
  readonly outcome: SagaReplyOutcome;
  /**
   * Выход шага (объект: литерал, интерфейс, экземпляр класса) — вливается в
   * контекст. Только при `ok` фазы выполнения.
   */
  readonly data?: object | null;
  /** Причина отказа (`failed`). */
  readonly error?: string;
  /** `false` — без повторов (окончательный отказ). По умолчанию повтор. */
  readonly retryable?: boolean;
}
