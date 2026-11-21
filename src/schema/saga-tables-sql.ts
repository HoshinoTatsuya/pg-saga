import type { SagaTablesOptions } from '../types/saga-tables-options.type';
import { SAGA_CHECK_SQL } from './saga-check-sql';
import { SAGA_LAST_MOVE_SQL } from './saga-last-move-sql';
import { sagaTableNamesOf } from './saga-table-names-of';

/**
 * DDL таблиц саги для ручной миграции — те же таблицы, что `defineSagaTables`
 * (сверка по каталогу базы — в спеке). Инструкции разделены
 * `--> statement-breakpoint`, как в миграциях drizzle-kit.
 *
 * Время — `timestamp with time zone`: значения пишутся часами процесса
 * параметром запроса, и смещение сохраняется при любой зоне сервера.
 * Журнал удаляется вместе с экземпляром (`ON DELETE CASCADE`).
 */
export const sagaTablesSql = (options: SagaTablesOptions): string => {
  const n = sagaTableNamesOf(options);
  // Таблица в своей схеме — квалифицированное имя; индексы и ограничения
  // ложатся в схему таблицы сами.
  const q = (table: string): string => (n.schema === undefined ? `"${table}"` : `"${n.schema}"."${table}"`);
  const time = 'timestamp with time zone';
  return [
    `CREATE TABLE ${q(n.instances)} (\n` +
      '\t"id" varchar(128) NOT NULL,\n' +
      '\t"name" varchar(64) NOT NULL,\n' +
      '\t"version" integer NOT NULL,\n' +
      '\t"steps" jsonb NOT NULL,\n' +
      '\t"status" varchar(16) NOT NULL,\n' +
      '\t"step_index" integer NOT NULL,\n' +
      '\t"attempt" integer NOT NULL,\n' +
      '\t"generation" integer DEFAULT 0 NOT NULL,\n' +
      '\t"context" jsonb NOT NULL,\n' +
      '\t"failure" text,\n' +
      '\t"last_error" text,\n' +
      `\t"created_at" ${time} NOT NULL,\n` +
      `\t"updated_at" ${time} NOT NULL,\n` +
      `\t"finished_at" ${time},\n` +
      `\t"wake_at" ${time},\n` +
      `\tCONSTRAINT "${n.instancesPkey}" PRIMARY KEY("id"),\n` +
      `\tCONSTRAINT "${n.instancesStateCheck}" CHECK (${SAGA_CHECK_SQL.instancesState})\n);`,
    `CREATE TABLE ${q(n.journal)} (\n` +
      '\t"id" bigint GENERATED ALWAYS AS IDENTITY NOT NULL,\n' +
      '\t"saga_id" varchar(128) NOT NULL,\n' +
      '\t"event" varchar(32) NOT NULL,\n' +
      '\t"phase" varchar(16),\n' +
      '\t"step_index" integer,\n' +
      '\t"step" varchar(64),\n' +
      '\t"attempt" integer,\n' +
      '\t"generation" integer NOT NULL,\n' +
      '\t"detail" text,\n' +
      `\t"at" ${time} NOT NULL,\n` +
      `\tCONSTRAINT "${n.journalPkey}" PRIMARY KEY("id"),\n` +
      `\tCONSTRAINT "${n.journalPhaseCheck}" CHECK (${SAGA_CHECK_SQL.journalPhase})\n);`,
    `ALTER TABLE ${q(n.journal)} ADD CONSTRAINT "${n.journalSagaFkey}" FOREIGN KEY ("saga_id") ` +
      `REFERENCES ${q(n.instances)}("id") ON DELETE cascade ON UPDATE no action;`,
    `CREATE INDEX "${n.instancesStatusIdx}" ON ${q(n.instances)} USING btree ("status","updated_at");`,
    `CREATE INDEX "${n.instancesCreatedIdx}" ON ${q(n.instances)} USING btree ("created_at");`,
    `CREATE INDEX "${n.instancesMoveIdx}" ON ${q(n.instances)} USING btree ("status",(${SAGA_LAST_MOVE_SQL}),"id");`,
    `CREATE INDEX "${n.journalSagaIdx}" ON ${q(n.journal)} USING btree ("saga_id","id");`,
  ]
    .join('\n--> statement-breakpoint\n')
    .concat('\n');
};
