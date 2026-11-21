import type { SagaJournalEntry } from '../types/saga-journal-entry.type';
import type { SagaJournalEvent } from '../types/saga-journal-event.type';
import type { SagaPhase } from '../types/saga-phase.type';
import { decodeTimestamp } from './decode-timestamp';

/** Строка журнала от драйвера pg (bigint id — строкой, как его отдаёт драйвер). */
export const decodeJournalEntry = (row: Record<string, unknown>): SagaJournalEntry => ({
  id: String(row.id),
  sagaId: String(row.saga_id),
  event: row.event as SagaJournalEvent,
  phase: (row.phase as SagaPhase | null) ?? null,
  stepIndex: row.step_index === null || row.step_index === undefined ? null : Number(row.step_index),
  step: (row.step as string | null) ?? null,
  attempt: row.attempt === null || row.attempt === undefined ? null : Number(row.attempt),
  generation: Number(row.generation),
  detail: (row.detail as string | null) ?? null,
  at: decodeTimestamp(row.at),
});
