import type { SAGA_REPLY_OUTCOME } from '../constants/saga-reply-outcome.constant';

export type SagaReplyOutcome = (typeof SAGA_REPLY_OUTCOME)[keyof typeof SAGA_REPLY_OUTCOME];
