import type { SagaPhase } from './saga-phase.type';

/**
 * Payload собственных событий саги (`saga.command`, `saga.timer`): какой шаг,
 * какая фаза, какая попытка и какое поколение экземпляра ждёт это сообщение.
 * `commandId` — id события и ключ inbox.
 */
export interface SagaCommandPayload {
  readonly commandId: string;
  readonly sagaId: string;
  readonly generation: number;
  readonly phase: SagaPhase;
  readonly stepIndex: number;
  readonly attempt: number;
}
