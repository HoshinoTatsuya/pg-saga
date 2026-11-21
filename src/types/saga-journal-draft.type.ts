import type { SagaJournalEntry } from './saga-journal-entry.type';

/** Запись журнала до вставки. */
export type SagaJournalDraft = Omit<SagaJournalEntry, 'id' | 'sagaId' | 'at'>;
