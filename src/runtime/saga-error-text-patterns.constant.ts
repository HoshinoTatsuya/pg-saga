import { deepFreeze } from '../deep-freeze';

/** Как текст отказа очищается от SQL с параметрами (см. `sagaErrorText`). */
export const SAGA_ERROR_TEXT = {
  /** Глубина цепочки `cause`: дальше — повторяющиеся обёртки, а не причина. */
  causeDepth: 5,
  /**
   * Обёртка драйвера над отказом запроса (`Failed query: <SQL>\nparams: <значения>`):
   * её текст несёт параметры бизнес-запроса — пишется причина из `cause`.
   */
  queryWrapper: /^Failed query:/,
  /** Хвост с параметрами — срезается у ЛЮБОГО сообщения (другой драйвер, смена формата). */
  paramsTail: /\n\s*params:[\s\S]*$/,
} as const;

deepFreeze(SAGA_ERROR_TEXT);
