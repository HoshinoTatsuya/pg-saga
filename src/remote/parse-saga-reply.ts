import { SAGA_REPLY_OUTCOME } from '../constants/saga-reply-outcome.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { parseSagaCommandPayload } from '../runtime/parse-saga-command-payload';
import { sagaErrorText } from '../runtime/saga-error-text';
import { sagaValueText } from '../runtime/saga-value-text';
import type { SagaReply } from '../types/saga-reply.type';
import type { SagaReplyOutcome } from '../types/saga-reply-outcome.type';

/** Разбор ответа участника: адрес — строго, `data` — объект или ничего. */
export const parseSagaReply = (payload: unknown): SagaReply => {
  const address = parseSagaCommandPayload('ответ', payload);
  const record = payload as Record<string, unknown>;
  const outcome = record.outcome;
  if (!(Object.values(SAGA_REPLY_OUTCOME) as unknown[]).includes(outcome)) {
    throw new SagaPayloadError(`ответ: outcome — ${sagaValueText(outcome)}`);
  }
  const data = record.data;
  if (data !== undefined && data !== null && (typeof data !== 'object' || Array.isArray(data))) {
    throw new SagaPayloadError('ответ: data — объект или ничего');
  }
  const retryable = record.retryable;
  if (retryable !== undefined && typeof retryable !== 'boolean') {
    throw new SagaPayloadError('ответ: retryable — boolean');
  }
  return {
    ...address,
    outcome: outcome as SagaReplyOutcome,
    ...(data === undefined || data === null ? {} : { data }),
    ...(record.error === undefined ? {} : { error: sagaErrorText(record.error) }),
    ...(retryable === undefined ? {} : { retryable }),
  };
};
