import type { SagaCommandDraft } from './saga-command-draft.type';
import type { SagaInstanceState } from './saga-instance-state.type';
import type { SagaJournalDraft } from './saga-journal-draft.type';
import type { SagaSignal } from './saga-signal.type';

/**
 * Переход экземпляра: новое состояние, записи журнала и следующая команда —
 * всё пишется ОДНОЙ транзакцией вместе с эффектами шага; хуки метрик — после
 * её фиксации.
 */
export interface SagaTransition {
  readonly state: SagaInstanceState;
  readonly journal: readonly SagaJournalDraft[];
  readonly command: SagaCommandDraft | null;
  readonly signals: readonly SagaSignal[];
}
