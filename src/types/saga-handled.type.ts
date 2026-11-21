import type { SAGA_HANDLED } from '../constants/saga-handled.constant';

export type SagaHandled = (typeof SAGA_HANDLED)[keyof typeof SAGA_HANDLED];
