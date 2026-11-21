import type { NewOutboxMessage } from '@hoshinotatsuya/outbox';

import { SAGA_AGGREGATE_TYPE } from '../constants/saga-aggregate-type.constant';
import { SAGA_EVENT_TYPE } from '../constants/saga-event-type.constant';
import { sagaMessageId } from '../runtime/saga-message-id';
import type { SagaReply } from '../types/saga-reply.type';

/**
 * Ответ как событие outbox — для участника в той же базе: запишите его в
 * транзакции участника вместе с эффектом шага, оркестратор получит его тем же
 * релеем. id детерминирован от команды: повторно записанный ответ не
 * задваивается.
 */
export const sagaReplyMessage = (
  reply: SagaReply,
  eventType: string = SAGA_EVENT_TYPE.reply,
): NewOutboxMessage => ({
  id: sagaMessageId.reply(reply.commandId),
  // Свой агрегат у каждого ответа: при FIFO по агрегату (outbox `ordering:
  // aggregate`) ответ в агрегате экземпляра ждал бы за отложенной командой
  // повтора — успех прежней попытки применялся бы только после паузы повтора
  // (у компенсации — до минут), а участник получал бы лишнюю команду.
  aggregateType: SAGA_AGGREGATE_TYPE.reply,
  aggregateId: sagaMessageId.reply(reply.commandId),
  eventType,
  payload: reply,
});
