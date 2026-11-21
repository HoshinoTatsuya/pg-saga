import type { SagaIndexNames } from './saga-index-names.type';

/** Форма таблиц (`defineSagaTables`, `sagaTablesSql`). */
export interface SagaTablesOptions {
  /** Экземпляры саг. */
  readonly instances: string;
  /** Журнал шагов. */
  readonly journal: string;
  /** Схема таблиц (по умолчанию — `search_path` соединения); та же, что `SagaTableNames.schema`. */
  readonly schema?: string;
  /**
   * Свои имена индексов и ограничений: производное имя длинной таблицы
   * длиннее 63 байт (отказ — Postgres обрезал бы его молча) или не по
   * конвенции проекта. Не заданные — производные.
   */
  readonly indexNames?: Partial<SagaIndexNames>;
}
