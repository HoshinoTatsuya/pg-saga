import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';

/**
 * Имя объекта Postgres: `[a-z_][a-z0-9_]*` и не длиннее 63 байт — длиннее
 * сервер обрезал бы молча, и производные имена индексов совпали бы.
 */
export const identifier = (field: string, value: unknown): string => {
  if (typeof value !== 'string' || !/^[a-z_][a-z0-9_]*$/.test(value)) {
    throw new SagaConfigError(field, `имя [a-z_][a-z0-9_]*, получено ${sagaValueText(value)}`);
  }
  if (Buffer.byteLength(value) > SAGA_LIMIT.identifierBytes) {
    throw new SagaConfigError(field, `длиннее ${SAGA_LIMIT.identifierBytes} байт: ${value}`);
  }
  return value;
};
