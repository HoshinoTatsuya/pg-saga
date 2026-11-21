/** Имена таблиц саги во время работы (`[a-z_][a-z0-9_]*`). */
export interface SagaTableNames {
  readonly instances: string;
  readonly journal: string;
  /** Схема; не задана — `search_path` соединения. */
  readonly schema?: string;
}
