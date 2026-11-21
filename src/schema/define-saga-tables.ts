import { sql } from 'drizzle-orm';
import {
  bigint,
  check,
  foreignKey,
  index,
  integer,
  jsonb,
  pgSchema,
  pgTable,
  primaryKey,
  text,
  timestamp,
  varchar,
} from 'drizzle-orm/pg-core';

import type { SagaTablesOptions } from '../types/saga-tables-options.type';
import { SAGA_CHECK_SQL } from './saga-check-sql';
import { SAGA_LAST_MOVE_SQL } from './saga-last-move-sql';
import { sagaTableNamesOf } from './saga-table-names-of';

/**
 * Таблицы саги для схемы drizzle потребителя — те же, что `sagaTablesSql`:
 * экземпляры и журнал шагов. Оркестратору схема не нужна (он пишет сырым
 * SQL), она нужна миграциям drizzle-kit и чтению из кода приложения.
 */
export const defineSagaTables = (options: SagaTablesOptions) => {
  const n = sagaTableNamesOf(options);
  // Своя схема — таблицы drizzle в ней же, что и DDL `sagaTablesSql`.
  const table = (n.schema === undefined ? pgTable : pgSchema(n.schema).table) as typeof pgTable;
  const time = (name: string) => timestamp(name, { withTimezone: true, mode: 'date' });
  const instances = table(
    n.instances,
    {
      id: varchar('id', { length: 128 }).notNull(),
      name: varchar('name', { length: 64 }).notNull(),
      version: integer('version').notNull(),
      // Массив имён — экземпляр версии 0.1.0; `{ names, stepKeyFormat, incarnation }` — текущей.
      steps: jsonb('steps')
        .$type<string[] | { names: string[]; stepKeyFormat: 2; incarnation: number }>()
        .notNull(),
      status: varchar('status', { length: 16 }).notNull(),
      stepIndex: integer('step_index').notNull(),
      attempt: integer('attempt').notNull(),
      generation: integer('generation').notNull().default(0),
      context: jsonb('context').$type<Record<string, unknown>>().notNull(),
      failure: text('failure'),
      lastError: text('last_error'),
      createdAt: time('created_at').notNull(),
      updatedAt: time('updated_at').notNull(),
      finishedAt: time('finished_at'),
      wakeAt: time('wake_at'),
    },
    (t) => [
      primaryKey({ name: n.instancesPkey, columns: [t.id] }),
      index(n.instancesStatusIdx).on(t.status, t.updatedAt),
      index(n.instancesCreatedIdx).on(t.createdAt),
      index(n.instancesMoveIdx).on(t.status, sql.raw(`(${SAGA_LAST_MOVE_SQL})`), t.id),
      check(n.instancesStateCheck, sql.raw(SAGA_CHECK_SQL.instancesState)),
    ],
  );
  const journal = table(
    n.journal,
    {
      id: bigint('id', { mode: 'bigint' }).generatedAlwaysAsIdentity().notNull(),
      sagaId: varchar('saga_id', { length: 128 }).notNull(),
      event: varchar('event', { length: 32 }).notNull(),
      phase: varchar('phase', { length: 16 }),
      stepIndex: integer('step_index'),
      step: varchar('step', { length: 64 }),
      attempt: integer('attempt'),
      generation: integer('generation').notNull(),
      detail: text('detail'),
      at: time('at').notNull(),
    },
    (t) => [
      primaryKey({ name: n.journalPkey, columns: [t.id] }),
      foreignKey({ name: n.journalSagaFkey, columns: [t.sagaId], foreignColumns: [instances.id] }).onDelete(
        'cascade',
      ),
      index(n.journalSagaIdx).on(t.sagaId, t.id),
      check(n.journalPhaseCheck, sql.raw(SAGA_CHECK_SQL.journalPhase)),
    ],
  );
  return { instances, journal };
};
