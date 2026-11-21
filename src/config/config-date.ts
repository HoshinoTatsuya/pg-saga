import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';

/**
 * Момент от вызывающего (`now` старта, `at` отмены, срок очистки, порог
 * среза): Date — по тегу, а не `instanceof` (Date из другого контекста тоже
 * принимается), — и не Invalid Date: иначе отказ сервера 22007 посреди
 * транзакции вызывающего или запрос по `NaN`.
 */
export const sagaDate = (field: string, value: unknown): Date => {
  if (Object.prototype.toString.call(value) !== '[object Date]' || Number.isNaN((value as Date).getTime())) {
    throw new SagaConfigError(field, `момент (Date), получено ${sagaValueText(value)}`);
  }
  return new Date((value as Date).getTime());
};
