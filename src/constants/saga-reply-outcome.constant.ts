import { deepFreeze } from '../deep-freeze';

/** Исход в ответе удалённого участника. */
export const SAGA_REPLY_OUTCOME = {
  ok: 'ok',
  failed: 'failed',
} as const;

deepFreeze(SAGA_REPLY_OUTCOME);
