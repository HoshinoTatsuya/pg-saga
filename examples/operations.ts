import type { OutboxStore } from '@hoshinotatsuya/outbox';
import { SagaAdmin, type SagaOrchestrator, SagaStallWatch, sagaTransactionRunner } from '@hoshinotatsuya/pg-saga';
import { sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

const tables = { instances: 'saga_instances', journal: 'saga_journal' };

// Cancel a running saga from application code, in the caller's transaction (like `start`). Set your own
// lock_timeout: the instance row may be held by a running step.
export const cancelOrder = (db: NodePgDatabase, orchestrator: SagaOrchestrator, orderId: string) =>
  db.transaction(async (tx) => {
    await tx.execute(sql`SET LOCAL lock_timeout = '5s'`);
    return orchestrator.cancel(tx, `order:${orderId}`, 'customer cancelled the order');
    // { cancelled: true, status: 'compensating' } or { cancelled: false, status } when not running
  });

// Manual repair of stuck/blocked/stalled instances (the `saga-stuck` CLI does the same from a shell).
export const repair = async (
  db: NodePgDatabase,
  outbox: OutboxStore,
  orchestrator: SagaOrchestrator,
  id: string,
) => {
  const admin = new SagaAdmin({
    db,
    transaction: sagaTransactionRunner(db),
    outbox,
    tables,
    commandEventType: orchestrator.eventTypes.command, // required by retry
  });
  const stuck = await admin.stuckReport(); // stuck and blocked: what failed, on which step, for how long
  const stalled = await admin.stalledReport(30 * 60_000); // active, not moved for >= 30 min
  const shown = await admin.show(id); // instance + journal in order
  await admin.retry(id, 'warehouse is back'); // stuck/blocked: retry the step in its phase, new generation
  await admin.retry(id, 'amount fixed', { contextPatch: { amount: 100 } }); // patch the context first
  await admin.retry(id, 'dead letter handled', { stalledForMs: 30 * 60_000 }); // a stalled active instance
  await admin.resolve(id, 'refund issued by hand'); // terminal `resolved`, the note goes to the journal
  const removed = await admin.prune(new Date(Date.now() - 30 * 86_400_000)); // finished ones, journal cascades
  return { stuck, stalled, shown, removed, stats: await admin.stats(new Date(Date.now() - 86_400_000)) };
};

// The stall watch moves an instance whose expected message went DEAD in the outbox to `blocked`.
export const runStallWatch = (db: NodePgDatabase, outbox: OutboxStore): NodeJS.Timeout => {
  const watch = new SagaStallWatch({
    db,
    transaction: sagaTransactionRunner(db),
    outbox,
    tables,
    stalledAfterMs: 30 * 60_000,
    metrics: {
      blocked: (event) => console.warn('saga blocked', event.sagaId, event.error),
      stalled: (event) => console.warn('saga stalled', event.sagaId, event.stalledForMs),
    },
  });
  return setInterval(() => {
    watch.runOnce().catch((error: unknown) => console.error('stall watch failed', error));
  }, 60_000);
};
