import type { OutboxDeliveryPort, OutboxHandlers, OutboxInbox, OutboxStore } from '@hoshinotatsuya/outbox';
import { parseSagaCommand, sagaReplyMessage, sagaReplyOf } from '@hoshinotatsuya/pg-saga';
import { eq } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { pgTable, text } from 'drizzle-orm/pg-core';

const charges = pgTable('charges', { stepKey: text('step_key').primaryKey(), chargeId: text('charge_id').notNull() });

/**
 * A participant of the remote step `charge` living in the same database: its effect is applied once per
 * `stepKey` (inbox), and the reply is an outbox event written in the same transaction as the effect.
 */
export const billingParticipant = (db: NodePgDatabase, outbox: OutboxStore, inbox: OutboxInbox) => {
  const charge: OutboxDeliveryPort = async (delivery) => {
    const { meta } = parseSagaCommand(delivery.payload);
    await db.transaction(async (tx) => {
      await inbox.once(tx, 'billing', meta.stepKey, async () => {
        await tx.insert(charges).values({ stepKey: meta.stepKey, chargeId: `ch_${meta.sagaId}` });
      });
      // Reply to a duplicate as well (same outcome again): duplicates arrive after a reply timeout and
      // after a manual retry, and silence would leave the saga waiting for a reply that never comes.
      const [row] = await tx.select().from(charges).where(eq(charges.stepKey, meta.stepKey));
      const reply =
        row === undefined
          ? sagaReplyOf(meta, { ok: false, error: 'charge not recorded', retryable: true })
          : sagaReplyOf(meta, { ok: true, data: { chargeId: row.chargeId } });
      await outbox.enqueue(tx, [sagaReplyMessage(reply)]);
    });
  };
  const refund: OutboxDeliveryPort = async (delivery) => {
    const { meta } = parseSagaCommand(delivery.payload);
    await db.transaction(async (tx) => {
      // The compensation must tolerate "nothing to undo": after a reply timeout the charge may not exist.
      await tx.delete(charges).where(eq(charges.stepKey, meta.executeStepKey ?? meta.stepKey));
      await outbox.enqueue(tx, [sagaReplyMessage(sagaReplyOf(meta, { ok: true }))]);
    });
  };
  return { charge, refund };
};

export const registerBilling = (
  handlers: OutboxHandlers,
  ports: ReturnType<typeof billingParticipant>,
): OutboxHandlers =>
  handlers
    .register('billing.charge', (_payload, delivery, context) => ports.charge(delivery, context))
    .register('billing.refund', (_payload, delivery, context) => ports.refund(delivery, context));
