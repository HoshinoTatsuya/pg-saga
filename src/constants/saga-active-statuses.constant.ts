import { deepFreeze } from '../deep-freeze';
import type { SagaStatus } from '../types/saga-status.type';

/** Состояния, в которых экземпляр двигается и ждёт сообщений саги. */
export const SAGA_ACTIVE_STATUSES: readonly SagaStatus[] = ['running', 'compensating'];

deepFreeze(SAGA_ACTIVE_STATUSES);
