import { deepFreeze } from '../deep-freeze';

/**
 * Состояния экземпляра саги. Активные — `running` (идут шаги) и
 * `compensating` (идут компенсации); конечные — `completed`, `compensated`
 * (шаг отказал, завершённые шаги компенсированы) и `resolved` (застрявший
 * разобран вручную). `stuck` — компенсация исчерпала повторы: экземпляр ждёт
 * человека и сам не двигается. `blocked` — доставка сообщения, которого
 * экземпляр ждал (команда шага или таймер ответа), ушла в DEAD outbox: сам он
 * не сдвинется никогда, ждёт человека (`SagaAdmin.retry` — шаг в той фазе, где
 * встал; `resolve`). Ставит его `SagaStallWatch`.
 */
export const SAGA_STATUS = {
  running: 'running',
  compensating: 'compensating',
  completed: 'completed',
  compensated: 'compensated',
  stuck: 'stuck',
  resolved: 'resolved',
  blocked: 'blocked',
} as const;

deepFreeze(SAGA_STATUS);
