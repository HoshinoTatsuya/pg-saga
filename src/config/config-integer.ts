import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';

/** Целое от `min` до int4: настройка, которая уходит в колонку или в таймер. */
export const integer = (field: string, value: unknown, min: number): number => {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > SAGA_LIMIT.int4Max) {
    throw new SagaConfigError(
      field,
      `целое от ${min} до ${SAGA_LIMIT.int4Max}, получено ${sagaValueText(value)}`,
    );
  }
  return value;
};
