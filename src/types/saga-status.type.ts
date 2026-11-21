import type { SAGA_STATUS } from '../constants/saga-status.constant';

export type SagaStatus = (typeof SAGA_STATUS)[keyof typeof SAGA_STATUS];
