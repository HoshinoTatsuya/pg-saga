import {
  inboxTableSql,
  OUTBOX_TIMESTAMP,
  OutboxHandlers,
  OutboxInbox,
  OutboxRelay,
  OutboxStore,
  outboxTableSql,
} from '@hoshinotatsuya/outbox';
import { defineSaga, SagaOrchestrator, sagaTablesSql } from '@hoshinotatsuya/pg-saga';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { Pool } from 'pg';

// 1. Migration: the outbox and inbox tables come from the outbox package, the saga tables from this one.
export const migrationSql = [
  outboxTableSql({ name: 'app_outbox', timestamps: OUTBOX_TIMESTAMP.zoned }),
  inboxTableSql({ name: 'app_inbox', timestamps: OUTBOX_TIMESTAMP.zoned }),
  sagaTablesSql({ instances: 'saga_instances', journal: 'saga_journal' }),
  'CREATE TABLE IF NOT EXISTS reservations (id serial PRIMARY KEY, order_id text NOT NULL)',
].join(';\n');

// 2. Definition: a local step (runs in the saga's transaction) and a remote step (a command to a participant).
// `tx` is typed as an executor (`tx.execute(sql)`); with the default runner it is a drizzle transaction, so
// `(tx as unknown as NodePgDatabase).insert(...)` works as well.
export const orderSaga = defineSaga<{ orderId: string }>('order', { version: 1 })
  .step('reserve', {
    execute: async ({ tx, context }) => {
      const { rows } = await tx.execute(
        sql`INSERT INTO reservations (order_id) VALUES (${context.orderId}) RETURNING id`,
      );
      const [row] = rows as ReadonlyArray<{ id: number }>;
      if (row === undefined) {
        throw new Error('reservation was not inserted');
      }
      return { reservationId: row.id }; // merged into the context of the next steps
    },
    compensate: async ({ tx, context }) => {
      await tx.execute(sql`DELETE FROM reservations WHERE id = ${context.reservationId}`);
    },
    retry: { maxAttempts: 3, backoffMs: 1_000 },
    timeoutMs: 10_000,
  })
  .remoteStep<{ chargeId: string }>('charge', {
    command: ({ context, stepKey }) => ({
      eventType: 'billing.charge',
      payload: { orderId: context.orderId, idempotencyKey: stepKey },
    }),
    compensation: ({ executeStepKey }) => ({ eventType: 'billing.refund', payload: { executeStepKey } }),
    timeoutMs: 60_000,
  })
  .build();

// 3. Wiring: one outbox store, the orchestrator, the outbox handlers and a relay over the SAME store.
export const startSagaRuntime = (pool: Pool) => {
  const db = drizzle(pool);
  const outbox = new OutboxStore(db, { table: 'app_outbox' });
  const orchestrator = new SagaOrchestrator({
    db,
    outbox,
    inbox: new OutboxInbox(db, { table: 'app_inbox' }),
    tables: { instances: 'saga_instances', journal: 'saga_journal' },
  }).register(orderSaga);

  const handlers = new OutboxHandlers();
  orchestrator.registerHandlers(handlers); // saga.command, saga.timer, saga.reply
  // Remote participants are ordinary outbox handlers (see examples/participant.ts).
  const relay = new OutboxRelay({ store: outbox, deliver: handlers.deliver });
  relay.start(1_000);
  return { db, outbox, orchestrator, handlers, relay };
};

// 4. Start: inside the caller's transaction, so the instance and its first command appear with the order row.
export const placeOrder = async (
  { db, orchestrator }: ReturnType<typeof startSagaRuntime>,
  orderId: string,
): Promise<boolean> => {
  const result = await db.transaction((tx) =>
    orchestrator.start(tx, orderSaga, { id: `order:${orderId}`, input: { orderId } }),
  );
  return result.started; // false: an instance with this id already exists
};
