import { sql } from 'drizzle-orm';

import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { parseSagaCommandPayload } from '../runtime/parse-saga-command-payload';
import { sagaMessageId } from '../runtime/saga-message-id';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import type { SagaDefinition } from '../types/saga-definition.type';
import type { SagaReplica } from './saga-rig';
import { TEST_TABLES } from './saga-test-database';

/**
 * Разбор команды на стороне участника — ровно как в версии 0.1.0: ключ шага
 * обязан быть `<sagaId>:<фаза>:<номер>` (формат 1), прочие поля служебной части
 * не смотрятся. Фикстура смешанного флота: участник, ещё не обновлённый до
 * 0.2.0, получает команды оркестратора 0.2.0.
 */
export const parseSagaCommandV010 = (
  payload: unknown,
): { readonly meta: SagaCommandMeta; readonly data: unknown } => {
  if (typeof payload !== 'object' || payload === null || !('sagaCommand' in payload)) {
    throw new SagaPayloadError('команда участнику: нет sagaCommand');
  }
  const { sagaCommand, data } = payload as { sagaCommand: Record<string, unknown>; data: unknown };
  const address = parseSagaCommandPayload('команда участнику', sagaCommand);
  const { saga, step, version, stepKey } = sagaCommand;
  for (const [field, value] of [
    ['saga', saga],
    ['step', step],
  ] as const) {
    if (typeof value !== 'string' || value.length === 0 || value.length > SAGA_LIMIT.nameLength) {
      throw new SagaPayloadError(`команда участнику: ${field} — ${String(value)}`);
    }
  }
  if (typeof version !== 'number' || !Number.isInteger(version) || version < 1) {
    throw new SagaPayloadError(`команда участнику: version — ${String(version)}`);
  }
  if (stepKey !== `${address.sagaId}:${address.phase}:${address.stepIndex}`) {
    throw new SagaPayloadError(`команда участнику: stepKey не сходится с адресом — ${String(stepKey)}`);
  }
  return {
    meta: { ...address, saga: saga as string, step: step as string, version, stepKey },
    data,
  };
};

/**
 * Экземпляр, каким его начинает версия 0.1.0: `steps` — массив имён, первая
 * команда — с id её формата (без воплощения) в агрегате экземпляра.
 */
export const startAsV010 = async (
  replica: SagaReplica,
  definition: SagaDefinition<object, object>,
  id: string,
  input: object,
): Promise<void> => {
  const now = replica.clock.now;
  const commandId = sagaMessageId.command({
    sagaId: id,
    incarnation: null,
    generation: 0,
    phase: 'execute',
    stepIndex: 0,
    attempt: 1,
  });
  await replica.transaction(async (tx) => {
    await tx.execute(
      sql`INSERT INTO ${sql.identifier(TEST_TABLES.instances)}
            (id, name, version, steps, status, step_index, attempt, generation, context, created_at, updated_at)
          VALUES (${id}, ${definition.name}, ${definition.version},
                  ${JSON.stringify(definition.steps.map((step) => step.name))}::jsonb,
                  'running', 0, 1, 0, ${JSON.stringify(input)}::jsonb, ${now}, ${now})`,
    );
    await replica.outbox.enqueue(
      tx,
      [
        {
          id: commandId,
          aggregateType: 'saga',
          aggregateId: id,
          eventType: replica.orchestrator.eventTypes.command,
          payload: { commandId, sagaId: id, generation: 0, phase: 'execute', stepIndex: 0, attempt: 1 },
        },
      ],
      now,
    );
  });
};
