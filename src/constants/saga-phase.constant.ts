import { deepFreeze } from '../deep-freeze';

/** Фаза команды: выполнение шага или его компенсация. */
export const SAGA_PHASE = {
  execute: 'execute',
  compensate: 'compensate',
} as const;

deepFreeze(SAGA_PHASE);
