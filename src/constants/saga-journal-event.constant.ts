import { deepFreeze } from '../deep-freeze';

/** События журнала экземпляра (колонка `event`, varchar(32)). */
export const SAGA_JOURNAL_EVENT = {
  sagaStarted: 'saga-started',
  stepDispatched: 'step-dispatched',
  stepCompleted: 'step-completed',
  stepFailed: 'step-failed',
  stepTimedOut: 'step-timed-out',
  stepCompensated: 'step-compensated',
  compensationFailed: 'compensation-failed',
  sagaCompleted: 'saga-completed',
  sagaCompensated: 'saga-compensated',
  sagaStuck: 'saga-stuck',
  lateReply: 'late-reply',
  /** Исход шага неизвестен, а компенсации у него нет: эффект у участника мог остаться. */
  uncompensatedUncertain: 'uncompensated-uncertain',
  manualRetry: 'manual-retry',
  manualResolve: 'manual-resolve',
  /** Сага отменена вызывающим (`orchestrator.cancel`), дальше — компенсации. */
  sagaCancelled: 'saga-cancelled',
  /** Сообщение, которого ждал экземпляр, ушло в DEAD outbox: экземпляр `blocked`. */
  deliveryDead: 'delivery-dead',
} as const;

deepFreeze(SAGA_JOURNAL_EVENT);
