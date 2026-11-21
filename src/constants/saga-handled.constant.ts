import { deepFreeze } from '../deep-freeze';

/**
 * Итог обработки одного сообщения саги:
 * - `applied` — шаг (или компенсация) выполнен или ответ применён, сага сдвинулась;
 * - `failure-recorded` — шаг отказал: записан повтор или начаты компенсации;
 * - `dispatched` — команда удалённому участнику записана в outbox;
 * - `duplicate` — сообщение уже обработано (inbox): повторная доставка;
 * - `stale` — экземпляр ждёт другого (шаг сдвинулся, попытка сменилась,
 *   сага завершена): сообщение ничего не меняет.
 */
export const SAGA_HANDLED = {
  applied: 'applied',
  failureRecorded: 'failure-recorded',
  dispatched: 'dispatched',
  duplicate: 'duplicate',
  stale: 'stale',
} as const;

deepFreeze(SAGA_HANDLED);
