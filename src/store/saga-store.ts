import type { OutboxExecutor } from '@hoshinotatsuya/outbox';
import { type SQL, sql } from 'drizzle-orm';
import { sagaDate } from '../config/config-date';
import { integer } from '../config/config-integer';
import { SAGA_ACTIVE_STATUSES } from '../constants/saga-active-statuses.constant';
import { SAGA_FINISHED_STATUSES } from '../constants/saga-finished-statuses.constant';
import { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';
import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { SAGA_LAST_MOVE_SQL } from '../schema/saga-last-move-sql';
import type { SagaContext } from '../types/saga-context.type';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaInstanceState } from '../types/saga-instance-state.type';
import type { SagaJournalDraft } from '../types/saga-journal-draft.type';
import type { SagaJournalEntry } from '../types/saga-journal-entry.type';
import type { SagaListFilter } from '../types/saga-list-filter.type';
import type { SagaReply } from '../types/saga-reply.type';
import type { SagaStats } from '../types/saga-stats.type';
import type { SagaStepFacts } from '../types/saga-step-facts.type';
import type { SagaTableNames } from '../types/saga-table-names.type';
import { decodeJournalEntry } from './decode-journal-entry';
import { decodeSagaInstance } from './decode-saga-instance';
import { decodeTimestamp } from './decode-timestamp';
import { sagaTableRef } from './saga-table-ref';

/**
 * SQL таблиц саги. Время — часы ПРОЦЕССА параметром запроса, ни одного
 * `now()` сервера: переходы и проверки в спеках идут по одной оси времени.
 * Запись — только в переданную транзакцию: состояние, журнал и команда
 * пишутся вместе с эффектами шага или не пишутся вовсе.
 */
export class SagaStore {
  private readonly _instances: SQL;

  private readonly _journal: SQL;

  public constructor(tables: SagaTableNames) {
    this._instances = sagaTableRef(tables.instances, tables.schema);
    this._journal = sagaTableRef(tables.journal, tables.schema);
  }

  /** Новый экземпляр; уже есть с этим id — `false`, ничего не пишется. */
  public async insert(
    tx: OutboxExecutor,
    row: {
      readonly id: string;
      readonly name: string;
      readonly version: number;
      readonly steps: readonly string[];
      readonly incarnation: number;
      readonly context: SagaContext;
    },
    now: Date,
  ): Promise<boolean> {
    const result = await tx.execute(
      sql`INSERT INTO ${this._instances}
            (id, name, version, steps, status, step_index, attempt, generation, context, created_at, updated_at)
          VALUES (${row.id}, ${row.name}, ${row.version},
                  ${JSON.stringify({ names: row.steps, stepKeyFormat: 2, incarnation: row.incarnation })}::jsonb,
                  ${SAGA_STATUS.running}, 0, 1, 0, ${JSON.stringify(row.context)}::jsonb, ${now}, ${now})
          ON CONFLICT (id) DO NOTHING`,
    );
    return (result.rowCount ?? 0) > 0;
  }

  /**
   * Экземпляр под блокировкой строки до конца транзакции: второй исполнитель
   * того же экземпляра ждёт здесь и читает уже сдвинутое состояние.
   */
  public async lock(tx: OutboxExecutor, id: string): Promise<SagaInstance | null> {
    const result = await tx.execute(sql`SELECT * FROM ${this._instances} WHERE id = ${id} FOR UPDATE`);
    const row = result.rows[0] as Record<string, unknown> | undefined;
    return row === undefined ? null : decodeSagaInstance(row);
  }

  public async get(db: OutboxExecutor, id: string): Promise<SagaInstance | null> {
    const result = await db.execute(sql`SELECT * FROM ${this._instances} WHERE id = ${id}`);
    const row = result.rows[0] as Record<string, unknown> | undefined;
    return row === undefined ? null : decodeSagaInstance(row);
  }

  /**
   * Записать состояние перехода целиком. `wakeAt` — срок отложенной команды
   * перехода (пауза повтора); нет — экземпляр ждёт немедленного сообщения.
   */
  public async update(
    tx: OutboxExecutor,
    id: string,
    state: SagaInstanceState,
    now: Date,
    wakeAt: Date | null = null,
  ): Promise<void> {
    const result = await tx.execute(
      sql`UPDATE ${this._instances}
             SET status = ${state.status}, step_index = ${state.stepIndex}, attempt = ${state.attempt},
                 generation = ${state.generation}, context = ${JSON.stringify(state.context)}::jsonb,
                 failure = ${state.failure}, last_error = ${state.lastError},
                 finished_at = ${state.finishedAt}, wake_at = ${wakeAt}, updated_at = ${now}
           WHERE id = ${id}`,
    );
    if (result.rowCount !== 1) {
      throw new Error(`pg-saga: экземпляр ${id} не обновился (строк: ${String(result.rowCount)})`);
    }
  }

  /** Команда участнику ушла: экземпляр ждёт ответа до срока таймера. */
  public async awaitUntil(tx: OutboxExecutor, id: string, wakeAt: Date, now: Date): Promise<void> {
    await tx.execute(
      sql`UPDATE ${this._instances} SET wake_at = ${wakeAt}, updated_at = ${now} WHERE id = ${id}`,
    );
  }

  public async append(
    tx: OutboxExecutor,
    sagaId: string,
    entries: readonly SagaJournalDraft[],
    now: Date,
  ): Promise<void> {
    if (entries.length === 0) {
      return;
    }
    const values = entries.map(
      (entry) =>
        sql`(${sagaId}, ${entry.event}, ${entry.phase}, ${entry.stepIndex}, ${entry.step}, ${entry.attempt},
             ${entry.generation}, ${entry.detail}, ${now})`,
    );
    await tx.execute(
      sql`INSERT INTO ${this._journal} (saga_id, event, phase, step_index, step, attempt, generation, detail, at)
          VALUES ${sql.join(values, sql`, `)}`,
    );
  }

  /** Журнал экземпляра по порядку записи. */
  public async journalOf(db: OutboxExecutor, id: string): Promise<SagaJournalEntry[]> {
    const result = await db.execute(sql`SELECT * FROM ${this._journal} WHERE saga_id = ${id} ORDER BY id`);
    return (result.rows as Record<string, unknown>[]).map(decodeJournalEntry);
  }

  /**
   * Что журнал знает о шаге неожиданного ответа (`sagaLateReply`): уходила ли
   * команда ровно этой попытки и поколения и выполнен ли шаг в этой фазе.
   */
  public async stepFacts(
    tx: OutboxExecutor,
    sagaId: string,
    reply: Pick<SagaReply, 'phase' | 'stepIndex' | 'generation' | 'attempt'>,
  ): Promise<SagaStepFacts> {
    const done =
      reply.phase === SAGA_PHASE.execute
        ? SAGA_JOURNAL_EVENT.stepCompleted
        : SAGA_JOURNAL_EVENT.stepCompensated;
    const result = await tx.execute(
      sql`SELECT EXISTS (SELECT 1 FROM ${this._journal}
                          WHERE saga_id = ${sagaId} AND event = ${SAGA_JOURNAL_EVENT.stepDispatched}
                            AND phase = ${reply.phase} AND step_index = ${reply.stepIndex}
                            AND generation = ${reply.generation} AND attempt = ${reply.attempt}) AS dispatched,
                 EXISTS (SELECT 1 FROM ${this._journal}
                          WHERE saga_id = ${sagaId} AND event = ${done} AND step_index = ${reply.stepIndex}) AS done`,
    );
    const row = result.rows[0] as { dispatched?: unknown; done?: unknown } | undefined;
    return { dispatched: row?.dispatched === true, done: row?.done === true };
  }

  /**
   * Была ли у шага в этом поколении попытка с неизвестным исходом
   * (`step-timed-out` фазы выполнения): участник мог её выполнить.
   */
  public async stepTimedOut(
    tx: OutboxExecutor,
    sagaId: string,
    stepIndex: number,
    generation: number,
  ): Promise<boolean> {
    const result = await tx.execute(
      sql`SELECT EXISTS (SELECT 1 FROM ${this._journal}
                          WHERE saga_id = ${sagaId} AND event = ${SAGA_JOURNAL_EVENT.stepTimedOut}
                            AND phase = ${SAGA_PHASE.execute} AND step_index = ${stepIndex}
                            AND generation = ${generation}) AS timed_out`,
    );
    return (result.rows[0] as { timed_out?: unknown } | undefined)?.timed_out === true;
  }

  public async list(db: OutboxExecutor, filter: SagaListFilter): Promise<SagaInstance[]> {
    const limit = integer('limit', filter.limit ?? 100, 1);
    if (limit > SAGA_LIMIT.listLimit) {
      throw new SagaConfigError(
        'limit',
        `не больше ${SAGA_LIMIT.listLimit} строк за срез, получено ${limit}`,
      );
    }
    const conditions: SQL[] = [sql`TRUE`];
    if (filter.status !== undefined) {
      conditions.push(sql`status = ${filter.status}`);
    }
    if (filter.saga !== undefined) {
      conditions.push(sql`name = ${filter.saga}`);
    }
    if (filter.statuses !== undefined) {
      conditions.push(
        filter.statuses.length === 0
          ? sql`FALSE`
          : sql`status IN (${sql.join(
              filter.statuses.map((status) => sql`${status}`),
              sql`, `,
            )})`,
      );
    }
    if (filter.updatedBefore !== undefined) {
      sagaDate('updatedBefore', filter.updatedBefore);
      // Последнее движение — позже из «сдвинулся» и «ждёт до»: ждущий паузы
      // повтора или таймера ответа не замёрз, пока срок не наступил.
      conditions.push(sql`${sql.raw(SAGA_LAST_MOVE_SQL)} < ${filter.updatedBefore}`);
    }
    // Старейшие по ПОСЛЕДНЕМУ ДВИЖЕНИЮ первыми — тем же выражением, что фильтр
    // замёрзших: экземпляр, давно сдвинутый, но ждущий таймера, — не старейший.
    const result = await db.execute(
      sql`SELECT * FROM ${this._instances} WHERE ${sql.join(conditions, sql` AND `)}
           ORDER BY ${sql.raw(SAGA_LAST_MOVE_SQL)}, id LIMIT ${limit}`,
    );
    return (result.rows as Record<string, unknown>[]).map(decodeSagaInstance);
  }

  /**
   * Активные, не двигавшиеся с `before` (последнее движение — позже из
   * `updated_at` и `wake_at`), после курсора `after` в порядке (движение, id),
   * до `limit`: проход сторожа по кругу (`SagaStallWatch`).
   */
  public async idleActive(
    db: OutboxExecutor,
    before: Date,
    after: { readonly at: Date; readonly id: string } | null,
    limit: number,
  ): Promise<SagaInstance[]> {
    const lastMove = sql.raw(SAGA_LAST_MOVE_SQL);
    const result = await db.execute(
      sql`SELECT * FROM ${this._instances}
           WHERE status IN (${sql.join(
             SAGA_ACTIVE_STATUSES.map((status) => sql`${status}`),
             sql`, `,
           )})
             AND ${lastMove} < ${before}
             ${after === null ? sql`` : sql`AND (${lastMove}, id) > (${after.at}::timestamptz, ${after.id})`}
           ORDER BY ${lastMove}, id LIMIT ${limit}`,
    );
    return (result.rows as Record<string, unknown>[]).map(decodeSagaInstance);
  }

  /** Последняя запись журнала с событием `event` в поколении `generation`. */
  public async lastEntry(
    tx: OutboxExecutor,
    sagaId: string,
    event: string,
    generation: number,
  ): Promise<SagaJournalEntry | null> {
    const result = await tx.execute(
      sql`SELECT * FROM ${this._journal}
           WHERE saga_id = ${sagaId} AND event = ${event} AND generation = ${generation}
           ORDER BY id DESC LIMIT 1`,
    );
    const row = result.rows[0] as Record<string, unknown> | undefined;
    return row === undefined ? null : decodeJournalEntry(row);
  }

  /** Срез по сагам: активные, застрявшие, начатые и завершённые с `since`, возраст старейшего активного. */
  public async stats(db: OutboxExecutor, now: Date, since: Date): Promise<SagaStats[]> {
    const result = await db.execute(
      sql`SELECT name,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.running})::int AS running,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.compensating})::int AS compensating,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.stuck})::int AS stuck,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.blocked})::int AS blocked,
                 count(*) FILTER (WHERE created_at >= ${since})::int AS started,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.completed} AND updated_at >= ${since})::int
                   AS completed,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.compensated} AND updated_at >= ${since})::int
                   AS compensated,
                 count(*) FILTER (WHERE status = ${SAGA_STATUS.resolved} AND updated_at >= ${since})::int
                   AS resolved,
                 min(created_at) FILTER (WHERE status IN (${sql.join(
                   SAGA_ACTIVE_STATUSES.map((status) => sql`${status}`),
                   sql`, `,
                 )})) AS oldest,
                 min(${sql.raw(SAGA_LAST_MOVE_SQL)})
                   FILTER (WHERE status IN (${SAGA_STATUS.running}, ${SAGA_STATUS.compensating})) AS idle
            FROM ${this._instances}
           WHERE status IN (${SAGA_STATUS.running}, ${SAGA_STATUS.compensating}, ${SAGA_STATUS.stuck},
                            ${SAGA_STATUS.blocked})
              OR (status IN (${SAGA_STATUS.completed}, ${SAGA_STATUS.compensated}, ${SAGA_STATUS.resolved})
                  AND updated_at >= ${since})
              OR created_at >= ${since}
           GROUP BY name
           ORDER BY name`,
    );
    return (result.rows as Record<string, unknown>[]).map((row) => ({
      saga: String(row.name),
      running: Number(row.running),
      compensating: Number(row.compensating),
      stuck: Number(row.stuck),
      blocked: Number(row.blocked),
      started: Number(row.started),
      completed: Number(row.completed),
      compensated: Number(row.compensated),
      resolved: Number(row.resolved),
      // Экземпляр начала реплика с часами впереди: возраст не бывает отрицательным.
      oldestActiveAgeMs:
        row.oldest === null ? null : Math.max(0, now.getTime() - decodeTimestamp(row.oldest).getTime()),
      longestIdleActiveMs:
        row.idle === null ? null : Math.max(0, now.getTime() - decodeTimestamp(row.idle).getTime()),
    }));
  }

  /** Удалить завершённые экземпляры (с журналом) старше `before` — до `limit` строк. */
  public async prune(db: OutboxExecutor, before: Date, limit: number): Promise<number> {
    const batch = integer('limit', limit, 1);
    // По статусу за проход: каждый — диапазон индекса (status, updated_at) в
    // порядке updated_at. `status IN (…три…) ORDER BY updated_at` индекс так не
    // обслуживает — каждая пачка читала таблицу целиком (замер: 1 млн строк).
    let deleted = 0;
    for (const status of SAGA_FINISHED_STATUSES) {
      if (deleted >= batch) {
        break;
      }
      const result = await db.execute(
        sql`WITH doomed AS MATERIALIZED (
              SELECT id FROM ${this._instances}
               WHERE status = ${status} AND updated_at < ${before}
               ORDER BY updated_at
               LIMIT ${batch - deleted}
               -- Параллельные очистки (cron на нескольких репликах) берут разные
               -- строки, а не ждут друг друга и не встают во взаимоблокировку.
               FOR UPDATE SKIP LOCKED
            )
            DELETE FROM ${this._instances} AS t USING doomed WHERE t.id = doomed.id`,
      );
      deleted += result.rowCount ?? 0;
    }
    return deleted;
  }
}
