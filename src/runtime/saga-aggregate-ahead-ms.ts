import { OUTBOX_STATUS, type OutboxExecutor } from '@hoshinotatsuya/outbox';
import { sql } from 'drizzle-orm';

import { sagaTableRef } from '../store/saga-table-ref';

/**
 * На сколько мс НЕДОСТАВЛЕННОЕ событие того же агрегата outbox записано позже
 * `own` (самое позднее из них); `null` — таких нет. FIFO outbox по агрегату
 * упорядочивает события по `(created_at, id)`: новое событие с более ранним
 * `created_at` (реплика с отстающими часами) или с тем же (неподвижные часы,
 * одна миллисекунда) встало бы ПЕРЕД ждущим — возврат обогнал бы списание.
 * Разность считает сервер: параметр приводится к типу колонки так же, как при
 * записи outbox (и для времени без зоны), а чтение не зависит от парсеров драйвера.
 */
export const sagaAggregateAheadMs = async (
  tx: OutboxExecutor,
  outbox: { readonly table: string; readonly schema: string | null },
  aggregate: { readonly type: string; readonly id: string },
  own: Date,
): Promise<number | null> => {
  const result = await tx.execute(
    sql`SELECT (EXTRACT(EPOCH FROM (max(created_at) - ${own})) * 1000)::float8 AS ahead
          FROM ${sagaTableRef(outbox.table, outbox.schema ?? undefined)}
         WHERE aggregate_type = ${aggregate.type} AND aggregate_id = ${aggregate.id}
           AND status IN (${OUTBOX_STATUS.pending}, ${OUTBOX_STATUS.dispatching})`,
  );
  const ahead = (result.rows[0] as { ahead?: unknown } | undefined)?.ahead;
  return ahead === null || ahead === undefined ? null : Number(ahead);
};
