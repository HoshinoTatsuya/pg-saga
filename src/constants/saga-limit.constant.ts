import { deepFreeze } from '../deep-freeze';

/** Ширина колонок: проверяется до записи, а не отказом сервера посреди транзакции. */
export const SAGA_LIMIT = {
  /** `saga_instances.id` и `saga_journal.saga_id` — varchar(128). */
  idLength: 128,
  /** Потолок строк одного среза (`list`, отчёты): больше — полный проход по таблице из CLI. */
  listLimit: 10_000,
  /** Имя саги и имя шага — varchar(64). */
  nameLength: 64,
  /** Имя объекта Postgres — 63 байта, длиннее сервер обрезает молча. */
  identifierBytes: 63,
  /** Целое int4 — версия, попытки, поколение. */
  int4Max: 2_147_483_647,
  /** Потолок длительностей API и CLI (порог простоя, окно среза, срок очистки), мс: десять лет. */
  durationMaxMs: 3_650 * 24 * 60 * 60 * 1_000,
} as const;

deepFreeze(SAGA_LIMIT);
