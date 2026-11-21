import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';

/**
 * Имя саги или шага: непустая строка до 64 знаков без управляющих символов и
 * без непарных суррогатов — имена уходят в jsonb (`steps`) и text: jsonb
 * непарный суррогат отвергает (22P02), text драйвер молча меняет на U+FFFD.
 */
export const sagaName = (field: string, value: unknown): string => {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > SAGA_LIMIT.nameLength ||
    /\p{Cc}/u.test(value) ||
    !value.isWellFormed()
  ) {
    throw new SagaConfigError(
      field,
      `непустая строка до ${SAGA_LIMIT.nameLength} знаков без управляющих символов и непарных суррогатов, получено ${sagaValueText(value)}`,
    );
  }
  return value;
};
