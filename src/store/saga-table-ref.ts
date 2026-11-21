import { type SQL, sql } from 'drizzle-orm';

import { identifier } from '../config/config-identifier';

/** Ссылка на таблицу для сырого SQL: со схемой или через `search_path`. */
export const sagaTableRef = (table: string, schema: string | undefined): SQL =>
  schema === undefined
    ? sql`${sql.identifier(identifier('table', table))}`
    : sql`${sql.identifier(identifier('schema', schema))}.${sql.identifier(identifier('table', table))}`;
