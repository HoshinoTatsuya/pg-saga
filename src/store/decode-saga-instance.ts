import type { SagaContext } from '../types/saga-context.type';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaStatus } from '../types/saga-status.type';
import { decodeSagaSteps } from './decode-saga-steps';
import { decodeTimestamp } from './decode-timestamp';

/** Строка `saga_instances` от драйвера pg (jsonb разобран, timestamptz — Date). */
export const decodeSagaInstance = (row: Record<string, unknown>): SagaInstance => {
  const createdAt = decodeTimestamp(row.created_at);
  const steps = decodeSagaSteps(row.steps, createdAt);
  return {
    id: String(row.id),
    name: String(row.name),
    version: Number(row.version),
    steps: steps.names,
    incarnation: steps.incarnation,
    status: row.status as SagaStatus,
    stepIndex: Number(row.step_index),
    attempt: Number(row.attempt),
    generation: Number(row.generation),
    context: row.context as SagaContext,
    failure: (row.failure as string | null) ?? null,
    lastError: (row.last_error as string | null) ?? null,
    createdAt,
    updatedAt: decodeTimestamp(row.updated_at),
    finishedAt:
      row.finished_at === null || row.finished_at === undefined ? null : decodeTimestamp(row.finished_at),
    wakeAt: row.wake_at === null || row.wake_at === undefined ? null : decodeTimestamp(row.wake_at),
  };
};
