import type { SagaCommandPayload } from '../types/saga-command-payload.type';
import type { SagaInstance } from '../types/saga-instance.type';
import { sagaMessageId } from './saga-message-id';

/**
 * Сообщение выдано ЭТИМ воплощением экземпляра: его id совпадает с
 * детерминированным id от адреса сообщения и воплощения экземпляра. Сверка
 * одних полей адреса пропускала сообщения прежнего воплощения того же id
 * (после `prune` и нового старта): таймер прежнего отваливал шаг нового по
 * таймауту, а задержанный ответ прежнего вливал в новый чужие данные.
 *
 * Экземпляр версии 0.1.0 (воплощения нет) выдаёт и принимает id её формата —
 * ровно те, что пишет и реплика 0.1.0 в смешанном флоте.
 */
export const sagaMessageBelongs = (
  kind: 'command' | 'remote' | 'timer',
  instance: Pick<SagaInstance, 'id' | 'incarnation'>,
  message: SagaCommandPayload,
): boolean =>
  message.sagaId === instance.id &&
  message.commandId ===
    sagaMessageId[kind]({
      sagaId: instance.id,
      incarnation: instance.incarnation,
      generation: message.generation,
      phase: message.phase,
      stepIndex: message.stepIndex,
      attempt: message.attempt,
    });
