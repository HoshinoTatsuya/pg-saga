import type { SagaPhase } from './saga-phase.type';

/**
 * Чем различаются сообщения саги: экземпляр, его воплощение, поколение, фаза,
 * шаг, попытка. `incarnation` — воплощение экземпляра (случайное целое при
 * старте): id саги можно занять снова после `prune`, и без него первая
 * команда нового экземпляра совпала бы с командой прежнего — outbox счёл бы
 * её дублем, inbox уже обработанной, и сага не поехала бы никогда. `null` —
 * формат id без воплощения: так их выдаёт экземпляр версии 0.1.0.
 */
export interface SagaMessageKey {
  readonly sagaId: string;
  readonly incarnation: number | null;
  readonly generation: number;
  readonly phase: SagaPhase;
  readonly stepIndex: number;
  readonly attempt: number;
}
