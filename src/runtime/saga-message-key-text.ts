import type { SagaMessageKey } from '../types/saga-message-key.type';

/**
 * Строка ключа для uuid v5: вид сообщения и поля адреса через разделитель
 * единиц (U+001F). Без воплощения (`incarnation: null`) — строка версии 0.1.0:
 * по ней узнаются летящие сообщения её экземпляров.
 */
export const sagaMessageKeyText = (kind: string, key: SagaMessageKey): string =>
  (key.incarnation === null
    ? [kind, key.sagaId, key.generation, key.phase, key.stepIndex, key.attempt]
    : [kind, key.sagaId, key.incarnation, key.generation, key.phase, key.stepIndex, key.attempt]
  ).join('\u001f');
