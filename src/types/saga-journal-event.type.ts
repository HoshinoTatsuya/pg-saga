import type { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';

export type SagaJournalEvent = (typeof SAGA_JOURNAL_EVENT)[keyof typeof SAGA_JOURNAL_EVENT];
