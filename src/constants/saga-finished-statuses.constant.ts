import { deepFreeze } from '../deep-freeze';
import type { SagaStatus } from '../types/saga-status.type';

/** Конечные состояния: экземпляр больше не двигается, его можно удалять по сроку. */
export const SAGA_FINISHED_STATUSES: readonly SagaStatus[] = ['completed', 'compensated', 'resolved'];

deepFreeze(SAGA_FINISHED_STATUSES);
