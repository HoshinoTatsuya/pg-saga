import { is, SQL } from 'drizzle-orm';
import { getTableConfig, type PgColumn, type PgTable } from 'drizzle-orm/pg-core';

import { SagaConfigError } from '../errors/saga-config.error';
import { createTestDatabase, describeDb, type TestDatabase } from '../testing/saga-test-database';
import type { SagaIndexNames } from '../types/saga-index-names.type';
import type { SagaTablesOptions } from '../types/saga-tables-options.type';
import { defineSagaTables } from './define-saga-tables';
import { sagaTablesSql } from './saga-tables-sql';

/** Колонка, как её видит каталог. */
interface CatalogColumn {
  name: string;
  type: string;
  notNull: boolean;
  hasDefault: boolean;
  identity: string;
}

/** Имя типа каталога → имя, которым его пишет drizzle (`getSQLType`). */
const drizzleTypeName = (catalog: string): string => catalog.replace(/^character varying/, 'varchar');

/** Таблица drizzle в форме каталога: колонки (с ТИПАМИ), PK, индексы, внешние ключи. */
const fromSchema = (table: PgTable) => {
  const config = getTableConfig(table);
  return {
    columns: config.columns
      .map((column) => ({
        name: column.name,
        type: column.getSQLType(),
        notNull: column.notNull,
        hasDefault: column.hasDefault && column.generatedIdentity === undefined,
        identity: column.generatedIdentity?.type === 'always' ? 'a' : '',
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    primaryKeys: [
      ...config.primaryKeys.map((key) => key.getName()),
      ...config.columns.filter((column) => column.primary).map(() => `${config.name}_pkey`),
    ],
    indexes: config.indexes
      .map((index) => ({
        name: String(index.config.name),
        // Выражение в индексе (последнее движение) — без имени колонки, как и в каталоге.
        columns: index.config.columns.map((column) =>
          is(column, SQL) ? '(выражение)' : (column as PgColumn).name,
        ),
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    checks: config.checks.map((check) => check.name).sort(),
    foreignKeys: config.foreignKeys.map((key) => {
      const reference = key.reference();
      return {
        name: key.getName(),
        columns: reference.columns.map((column) => column.name),
        target: getTableConfig(reference.foreignTable).name,
        targetColumns: reference.foreignColumns.map((column) => column.name),
        onDelete: key.onDelete ?? 'no action',
      };
    }),
  };
};

/**
 * Схема drizzle (`defineSagaTables`) и DDL ручной миграции (`sagaTablesSql`)
 * описывают одни таблицы: проверяется по каталогу базы, ВКЛЮЧАЯ типы колонок,
 * NOT NULL, DEFAULT, identity, PK, индексы и внешний ключ журнала.
 */
describeDb('Таблицы саги: схема drizzle и DDL', () => {
  let database: TestDatabase;

  const catalog = async (table: string) => {
    const { rows: columns } = await database.pool.query<CatalogColumn>(
      `SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS "notNull",
              (d.adbin IS NOT NULL) AS "hasDefault", a.attidentity::text AS identity
         FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attname`,
      [table],
    );
    const { rows: indexes } = await database.pool.query<{
      name: string;
      primary: boolean;
      columns: string[];
    }>(
      `SELECT c.relname AS name, i.indisprimary AS primary,
              array(SELECT COALESCE(a.attname::text, '(выражение)') FROM unnest(i.indkey) WITH ORDINALITY k(n, o)
                      LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.n ORDER BY k.o) AS columns
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = $1::regclass ORDER BY c.relname`,
      [table],
    );
    const { rows: foreignKeys } = await database.pool.query<{
      name: string;
      columns: string[];
      target: string;
      targetColumns: string[];
      onDelete: string;
    }>(
      `SELECT c.conname AS name,
              array(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(n, o)
                      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n ORDER BY k.o) AS columns,
              (SELECT t.relname FROM pg_class t WHERE t.oid = c.confrelid) AS target,
              array(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY k(n, o)
                      JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n ORDER BY k.o) AS "targetColumns",
              CASE c.confdeltype WHEN 'c' THEN 'cascade' WHEN 'a' THEN 'no action' ELSE c.confdeltype::text END
                AS "onDelete"
         FROM pg_constraint c WHERE c.conrelid = $1::regclass AND c.contype = 'f'`,
      [table],
    );
    const { rows: checks } = await database.pool.query<{ name: string }>(
      `SELECT conname AS name FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'c' ORDER BY conname`,
      [table],
    );
    return {
      columns: columns.map((column) => ({ ...column, type: drizzleTypeName(column.type) })),
      primaryKeys: indexes.filter((index) => index.primary).map((index) => index.name),
      indexes: indexes
        .filter((index) => !index.primary)
        .map((index) => ({ name: index.name, columns: index.columns })),
      checks: checks.map((check) => check.name),
      foreignKeys,
    };
  };

  beforeAll(async () => {
    database = await createTestDatabase();
  });

  afterAll(async () => {
    await database?.drop();
  });

  it.each([
    ['имена по умолчанию', { instances: 'saga_instances', journal: 'saga_journal' }],
    ['свои имена', { instances: 'checkout_sagas', journal: 'checkout_saga_steps' }],
    // Своя схема: DDL кладёт таблицы и внешний ключ в неё, drizzle — тоже.
    ['своя схема', { instances: 'saga_instances', journal: 'saga_journal', schema: 'saga_own' }],
    // Длинные имена таблиц: производные имена длиннее 63 байт — свои имена индексов и ограничений.
    [
      'длинные имена таблиц и свои имена индексов',
      {
        instances: `checkout_${'i'.repeat(48)}`,
        journal: `checkout_${'j'.repeat(48)}`,
        indexNames: {
          instancesPkey: 'co_sagas_pk',
          instancesStatusIdx: 'co_sagas_status_ix',
          instancesCreatedIdx: 'co_sagas_created_ix',
          instancesMoveIdx: 'co_sagas_move_ix',
          instancesStateCheck: 'co_sagas_state_ck',
          journalPhaseCheck: 'co_steps_phase_ck',
          journalPkey: 'co_steps_pk',
          journalSagaFkey: 'co_steps_saga_fk',
          journalSagaIdx: 'co_steps_saga_ix',
        },
      },
    ],
  ] as ReadonlyArray<
    readonly [
      string,
      { instances: string; journal: string; schema?: string; indexNames?: Partial<SagaIndexNames> },
    ]
  >)('%s: каталог совпадает со схемой drizzle у обеих таблиц', async (_label, options) => {
    if (options.schema !== undefined) {
      await database.pool.query(`CREATE SCHEMA "${options.schema}"`);
    }
    await database.pool.query(sagaTablesSql(options).replaceAll('--> statement-breakpoint', ''));
    const tables = defineSagaTables(options);
    const qualified = (table: string): string =>
      options.schema === undefined ? table : `${options.schema}.${table}`;

    const instances = await catalog(qualified(options.instances));
    const journal = await catalog(qualified(options.journal));

    expect(instances).toEqual(fromSchema(tables.instances));
    expect(journal).toEqual(fromSchema(tables.journal));
    // Сверка не выродилась: типы реально сравнивались, и у каждой таблицы есть что сравнить.
    expect(instances.columns.map((column) => [column.name, column.type])).toEqual(
      expect.arrayContaining([
        ['context', 'jsonb'],
        ['created_at', 'timestamp with time zone'],
        ['id', 'varchar(128)'],
        ['version', 'integer'],
      ]),
    );
    expect(journal.columns.find((column) => column.name === 'id')).toEqual({
      name: 'id',
      type: 'bigint',
      notNull: true,
      hasDefault: false,
      identity: 'a',
    });
    expect(journal.foreignKeys).toEqual([
      {
        name: options.indexNames?.journalSagaFkey ?? `${options.journal}_saga_id_fkey`,
        columns: ['saga_id'],
        target: options.instances,
        targetColumns: ['id'],
        onDelete: 'cascade',
      },
    ]);
    expect(instances.indexes).toHaveLength(3);
    expect({ instances: instances.checks, journal: journal.checks }).toEqual({
      instances: [options.indexNames?.instancesStateCheck ?? `${options.instances}_state_check`],
      journal: [options.indexNames?.journalPhaseCheck ?? `${options.journal}_phase_check`],
    });
    expect(instances.indexes.find((index) => index.columns.includes('(выражение)'))?.columns).toEqual([
      'status',
      '(выражение)',
      'id',
    ]);
    // Имена в каталоге — заданные (или производные), а не обрезанные сервером.
    expect({
      pkeys: [...instances.primaryKeys, ...journal.primaryKeys],
      indexes: [...instances.indexes, ...journal.indexes].map((index) => index.name).sort(),
    }).toEqual({
      pkeys: [
        options.indexNames?.instancesPkey ?? `${options.instances}_pkey`,
        options.indexNames?.journalPkey ?? `${options.journal}_pkey`,
      ],
      indexes: [
        options.indexNames?.instancesCreatedIdx ?? `${options.instances}_created_idx`,
        options.indexNames?.instancesMoveIdx ?? `${options.instances}_status_move_idx`,
        options.indexNames?.instancesStatusIdx ?? `${options.instances}_status_updated_idx`,
        options.indexNames?.journalSagaIdx ?? `${options.journal}_saga_idx`,
      ].sort(),
    });
    // Своя схема: таблицы и цель внешнего ключа — в ней, а не в search_path.
    const placed = await database.pool.query<{ schema: string }>(
      `SELECT DISTINCT n.nspname AS schema FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = ANY($1) AND c.relkind = 'r'`,
      [[options.instances, options.journal]],
    );
    expect(placed.rows.map((row) => row.schema)).toContain(options.schema ?? 'public');
    expect(instances.columns).toHaveLength(15);
    expect(journal.columns).toHaveLength(10);
  });

  it('имена вне [a-z_], одинаковые или с производным именем длиннее 63 байт — отказ и в DDL, и в схеме', () => {
    const long = `j${'x'.repeat(55)}`;
    const cases: ReadonlyArray<readonly [SagaTablesOptions, RegExp]> = [
      [{ instances: 'Sagas', journal: 'saga_journal' }, /instances/],
      [{ instances: 'same', journal: 'same' }, /journal/],
      // Подсказка — какое имя задать.
      [{ instances: 'saga_instances', journal: long }, /indexNames\.journalSagaFkey/],
      // Своё имя — то же правило, что у таблиц; неизвестный ключ — отказ, а не тихое производное имя.
      [
        { instances: 'a', journal: 'b', indexNames: { instancesPkey: 'Bad-Name' } },
        /indexNames\.instancesPkey/,
      ],
      [
        { instances: 'a', journal: 'b', indexNames: { instancesPk: 'a_pk' } as never },
        /indexNames\.instancesPk/,
      ],
      // Имя индекса совпало с таблицей или с другим индексом — отказ здесь, а не на середине миграции.
      [{ instances: 'a', journal: 'b', indexNames: { instancesCreatedIdx: 'b' } }, /instancesCreatedIdx/],
      [{ instances: 'a', journal: 'b', indexNames: { journalSagaIdx: 'a_pkey' } }, /journalSagaIdx/],
      [{ instances: 'a', journal: 'b', indexNames: { journalSagaFkey: 'b_pkey' } }, /journalSagaFkey/],
      // Одно длинное производное не спасено заданными другими.
      [
        { instances: 'saga_instances', journal: long, indexNames: { journalSagaFkey: 'j_fk' } },
        /indexNames\.journalSagaIdx/,
      ],
    ];
    for (const [options, field] of cases) {
      expect(() => sagaTablesSql(options)).toThrow(field);
      expect(() => defineSagaTables(options)).toThrow(SagaConfigError);
    }
    // Двойник: всё длинное названо — принято.
    expect(() =>
      sagaTablesSql({
        instances: 'saga_instances',
        journal: long,
        indexNames: { journalSagaIdx: 'j_ix', journalSagaFkey: 'j_fk', journalPhaseCheck: 'j_ck' },
      }),
    ).not.toThrow();
  });
});
