import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { parseSagaCommandPayload } from '../runtime/parse-saga-command-payload';
import { sagaValueText } from '../runtime/saga-value-text';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';

/**
 * Разбор команды удалённого шага на стороне участника: служебная часть
 * (`meta` — для ответа и для идемпотентности по `meta.stepKey`) и данные,
 * которые построил `command` шага.
 */
export const parseSagaCommand = (
  payload: unknown,
): { readonly meta: SagaCommandMeta; readonly data: unknown } => {
  if (typeof payload !== 'object' || payload === null || !('sagaCommand' in payload)) {
    throw new SagaPayloadError('команда участнику: нет sagaCommand');
  }
  const { sagaCommand, data } = payload as { sagaCommand: Record<string, unknown>; data: unknown };
  const address = parseSagaCommandPayload('команда участнику', sagaCommand);
  const { saga, step, version, stepKey, incarnation, executeStepKey } = sagaCommand;
  for (const [field, value] of [
    ['saga', saga],
    ['step', step],
  ] as const) {
    if (typeof value !== 'string' || value.length === 0 || value.length > SAGA_LIMIT.nameLength) {
      throw new SagaPayloadError(`команда участнику: ${field} — ${sagaValueText(value)}`);
    }
  }
  if (
    typeof version !== 'number' ||
    !Number.isInteger(version) ||
    version < 1 ||
    version > SAGA_LIMIT.int4Max
  ) {
    throw new SagaPayloadError(`команда участнику: version — ${sagaValueText(version)}`);
  }
  if (
    incarnation !== undefined &&
    (typeof incarnation !== 'number' || !Number.isSafeInteger(incarnation) || incarnation < 0)
  ) {
    throw new SagaPayloadError(`команда участнику: incarnation — ${sagaValueText(incarnation)}`);
  }
  // Ключ формата 2 — с воплощением (id саги можно занять снова после prune),
  // формата 1 — экземпляр прежней версии пакета.
  const keyOf = (phase: string): string =>
    incarnation === undefined
      ? `${address.sagaId}:${phase}:${address.stepIndex}`
      : `${address.sagaId}#${incarnation}:${phase}:${address.stepIndex}`;
  if (stepKey !== keyOf(address.phase)) {
    throw new SagaPayloadError(
      `команда участнику: stepKey не сходится с адресом — ${sagaValueText(stepKey)}`,
    );
  }
  if (executeStepKey !== undefined && executeStepKey !== keyOf('execute')) {
    throw new SagaPayloadError(
      `команда участнику: executeStepKey не сходится с адресом — ${sagaValueText(executeStepKey)}`,
    );
  }
  return {
    meta: {
      ...address,
      saga: saga as string,
      step: step as string,
      version,
      stepKey,
      ...(incarnation === undefined ? {} : { incarnation }),
      ...(executeStepKey === undefined ? {} : { executeStepKey }),
    },
    data,
  };
};
