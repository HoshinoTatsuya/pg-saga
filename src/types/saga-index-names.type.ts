/**
 * Имена индексов и ограничений таблиц саги (`SagaTablesOptions.indexNames`).
 * Не заданное — производное от имени таблицы (`<экземпляры>_pkey`,
 * `<экземпляры>_status_updated_idx`, `<экземпляры>_created_idx`,
 * `<экземпляры>_status_move_idx`, `<экземпляры>_state_check`, `<журнал>_pkey`,
 * `<журнал>_saga_id_fkey`, `<журнал>_saga_idx`, `<журнал>_phase_check`).
 */
export interface SagaIndexNames {
  /** Первичный ключ экземпляров (ограничение и его индекс). */
  readonly instancesPkey: string;
  /** Индекс экземпляров по `(status, updated_at)`. */
  readonly instancesStatusIdx: string;
  /** Индекс экземпляров по `created_at`. */
  readonly instancesCreatedIdx: string;
  /** Индекс экземпляров по `(status, последнее движение, id)`: замёрзшие и сторож. */
  readonly instancesMoveIdx: string;
  /** CHECK экземпляров: статус, `step_index >= 0`, `attempt >= 1`, `generation >= 0`. */
  readonly instancesStateCheck: string;
  /** Первичный ключ журнала (ограничение и его индекс). */
  readonly journalPkey: string;
  /** Внешний ключ журнала на экземпляр. */
  readonly journalSagaFkey: string;
  /** Индекс журнала по `(saga_id, id)`. */
  readonly journalSagaIdx: string;
  /** CHECK журнала: фаза известная или NULL. */
  readonly journalPhaseCheck: string;
}
