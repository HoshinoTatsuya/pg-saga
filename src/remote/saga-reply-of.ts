import { SAGA_REPLY_OUTCOME } from '../constants/saga-reply-outcome.constant';
import { sagaErrorText } from '../runtime/saga-error-text';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import type { SagaReply } from '../types/saga-reply.type';

/**
 * Ответ участника на команду: `{ ok: true, data }` — шаг выполнен (data
 * вольётся в контекст), `{ ok: false, error, retryable }` — отказ
 * (`retryable: false` — окончательный, без повторов).
 */
export const sagaReplyOf = (
  meta: SagaCommandMeta,
  result:
    | { readonly ok: true; readonly data?: object }
    | { readonly ok: false; readonly error: unknown; readonly retryable?: boolean },
): SagaReply => ({
  commandId: meta.commandId,
  sagaId: meta.sagaId,
  generation: meta.generation,
  phase: meta.phase,
  stepIndex: meta.stepIndex,
  attempt: meta.attempt,
  ...(result.ok
    ? { outcome: SAGA_REPLY_OUTCOME.ok, ...(result.data === undefined ? {} : { data: result.data }) }
    : {
        outcome: SAGA_REPLY_OUTCOME.failed,
        error: sagaErrorText(result.error),
        ...(result.retryable === undefined ? {} : { retryable: result.retryable }),
      }),
});
