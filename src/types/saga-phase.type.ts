import type { SAGA_PHASE } from '../constants/saga-phase.constant';

export type SagaPhase = (typeof SAGA_PHASE)[keyof typeof SAGA_PHASE];
