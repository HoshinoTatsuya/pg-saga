import type { SagaJournalEvent } from './saga-journal-event.type';
import type { SagaPhase } from './saga-phase.type';

/** Строка журнала шагов. */
export interface SagaJournalEntry {
  readonly id: string;
  readonly sagaId: string;
  readonly event: SagaJournalEvent;
  readonly phase: SagaPhase | null;
  readonly stepIndex: number | null;
  readonly step: string | null;
  readonly attempt: number | null;
  readonly generation: number;
  readonly detail: string | null;
  readonly at: Date;
}
