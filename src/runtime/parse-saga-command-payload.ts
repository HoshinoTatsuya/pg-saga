import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import type { SagaCommandPayload } from '../types/saga-command-payload.type';
import type { SagaPhase } from '../types/saga-phase.type';
import { payloadCount } from './payload-count';
import { sagaValueText } from './saga-value-text';

/**
 * Разбор адреса сообщения саги (команда, таймер, ответ): строго, каждое
 * поле. Сообщение не той формы — отказ доставки (outbox повторит и положит
 * в DEAD), а не «пропущено».
 */
export const parseSagaCommandPayload = (what: string, payload: unknown): SagaCommandPayload => {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
    throw new SagaPayloadError(`${what}: payload — не объект`);
  }
  const record = payload as Record<string, unknown>;
  const { sagaId, phase } = record;
  // Регистр uuid не важен: участник на платформе, форматирующей GUID заглавными,
  // вернёт id так. Ключ inbox — в нижнем регистре, как его выдал оркестратор.
  const commandId = typeof record.commandId === 'string' ? record.commandId.toLowerCase() : record.commandId;
  if (
    typeof commandId !== 'string' ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(commandId)
  ) {
    throw new SagaPayloadError(`${what}: commandId — не uuid: ${sagaValueText(record.commandId)}`);
  }
  if (typeof sagaId !== 'string' || sagaId.length === 0 || sagaId.length > SAGA_LIMIT.idLength) {
    throw new SagaPayloadError(`${what}: sagaId — не строка до ${SAGA_LIMIT.idLength} знаков`);
  }
  if (!(Object.values(SAGA_PHASE) as unknown[]).includes(phase)) {
    throw new SagaPayloadError(`${what}: phase — ${sagaValueText(phase)}`);
  }
  return {
    commandId,
    sagaId,
    phase: phase as SagaPhase,
    generation: payloadCount(`${what}: generation`, record.generation, 0),
    stepIndex: payloadCount(`${what}: stepIndex`, record.stepIndex, 0),
    attempt: payloadCount(`${what}: attempt`, record.attempt, 1),
  };
};
