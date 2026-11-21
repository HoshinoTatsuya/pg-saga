import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { sagaValueText } from './saga-value-text';

/** Целое поле сообщения саги от `min` до int4; иначе — отказ разбора. */
export const payloadCount = (what: string, value: unknown, min: number): number => {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > SAGA_LIMIT.int4Max) {
    throw new SagaPayloadError(`${what}: ${sagaValueText(value)} — не целое ≥ ${min}`);
  }
  return value;
};
