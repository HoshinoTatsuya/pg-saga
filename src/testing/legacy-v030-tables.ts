/**
 * DDL таблиц саги версии 0.3.0 (без CHECK и без индекса по последнему
 * движению) для имён по умолчанию в схеме `schema` — фикстура доводки
 * существующих таблиц `sagaUpgradeSql`; в сборку не входит.
 */
export const legacyV030TablesSql = (schema: string): string[] => {
  const q = (table: string): string => `"${schema}"."${table}"`;
  const time = 'timestamp with time zone';
  return [
    `CREATE TABLE ${q('saga_instances')} (
       "id" varchar(128) NOT NULL, "name" varchar(64) NOT NULL, "version" integer NOT NULL,
       "steps" jsonb NOT NULL, "status" varchar(16) NOT NULL, "step_index" integer NOT NULL,
       "attempt" integer NOT NULL, "generation" integer DEFAULT 0 NOT NULL, "context" jsonb NOT NULL,
       "failure" text, "last_error" text, "created_at" ${time} NOT NULL, "updated_at" ${time} NOT NULL,
       "finished_at" ${time}, "wake_at" ${time},
       CONSTRAINT "saga_instances_pkey" PRIMARY KEY("id"))`,
    `CREATE TABLE ${q('saga_journal')} (
       "id" bigint GENERATED ALWAYS AS IDENTITY NOT NULL, "saga_id" varchar(128) NOT NULL,
       "event" varchar(32) NOT NULL, "phase" varchar(16), "step_index" integer, "step" varchar(64),
       "attempt" integer, "generation" integer NOT NULL, "detail" text, "at" ${time} NOT NULL,
       CONSTRAINT "saga_journal_pkey" PRIMARY KEY("id"))`,
    `ALTER TABLE ${q('saga_journal')} ADD CONSTRAINT "saga_journal_saga_id_fkey" FOREIGN KEY ("saga_id")
       REFERENCES ${q('saga_instances')}("id") ON DELETE cascade ON UPDATE no action`,
    `CREATE INDEX "saga_instances_status_updated_idx" ON ${q('saga_instances')} USING btree ("status","updated_at")`,
    `CREATE INDEX "saga_instances_created_idx" ON ${q('saga_instances')} USING btree ("created_at")`,
    `CREATE INDEX "saga_journal_saga_idx" ON ${q('saga_journal')} USING btree ("saga_id","id")`,
  ];
};
