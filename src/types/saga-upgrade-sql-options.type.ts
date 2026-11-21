import type { SagaEventTypes } from './saga-event-types.type';
import type { SagaIndexNames } from './saga-index-names.type';

/** Таблицы и типы событий для `sagaUpgradeSql`. */
export interface SagaUpgradeSqlOptions {
  /** Таблица экземпляров саги. */
  readonly instances: string;
  /** Журнал саги: его CHECK фазы доводится тоже. */
  readonly journal: string;
  /** Свои имена индексов и ограничений — те же, что у `sagaTablesSql`. */
  readonly indexNames?: Partial<SagaIndexNames>;
  /** Таблица outbox, в которую пишет оркестратор. */
  readonly outbox: string;
  /** Схема таблиц саги (по умолчанию — `search_path`). */
  readonly schema?: string;
  /** Схема outbox; по умолчанию — `schema`. */
  readonly outboxSchema?: string;
  /** Свои типы событий оркестратора (`deps.eventTypes`), если заданы. */
  readonly eventTypes?: Partial<SagaEventTypes>;
  /**
   * Outbox со временем БЕЗ зоны (`OUTBOX_TIMESTAMP.naiveLocal`): зона процесса,
   * который его пишет (`Europe/Moscow`). Без неё время outbox считается
   * `timestamp with time zone` — для колонки без зоны Postgres тогда толкует
   * её значение в зоне СЕССИИ, и срок сдвигается на разницу зон.
   */
  readonly outboxNaiveTimeZone?: string;
}
