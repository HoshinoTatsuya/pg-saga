import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';

/**
 * Длительность, мс (порог простоя, окно среза): целое от 1 до
 * `SAGA_LIMIT.durationMaxMs`. В колонку она не пишется — int4 (≈ 24,8 суток)
 * ей не потолок; потолок общий у API и CLI.
 */
export const durationMs = (field: string, value: unknown): number => {
  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value < 1 ||
    value > SAGA_LIMIT.durationMaxMs
  ) {
    throw new SagaConfigError(
      field,
      `целое число мс от 1 до ${SAGA_LIMIT.durationMaxMs}, получено ${sagaValueText(value)}`,
    );
  }
  return value;
};
