import type { SagaMessageKey } from '../types/saga-message-key.type';
import { deterministicId } from './deterministic-id';
import { sagaMessageKeyText } from './saga-message-key-text';

/**
 * id сообщений саги — детерминированные от (экземпляр, воплощение, поколение,
 * фаза, шаг, попытка): команда исполнителю, команда удалённому участнику, таймер ответа
 * и ответ участника, записанный в тот же outbox. Разные виды — разные id и
 * разные ключи inbox.
 */
export const sagaMessageId = {
  command: (key: SagaMessageKey): string => deterministicId(sagaMessageKeyText('command', key)),
  remote: (key: SagaMessageKey): string => deterministicId(sagaMessageKeyText('remote', key)),
  timer: (key: SagaMessageKey): string => deterministicId(sagaMessageKeyText('timer', key)),
  reply: (commandId: string): string => deterministicId(`reply\u001f${commandId}`),
};
