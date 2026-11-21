import type { OutboxDeliveryPort } from '@hoshinotatsuya/outbox';

import { defineSaga } from '../definition/define-saga';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { sagaReplyMessage } from '../remote/saga-reply-message';
import { sagaReplyOf } from '../remote/saga-reply-of';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import { recordEffect, type SagaReplica } from './saga-rig';

/** Что участник делает с очередной командой фазы: ответить, отказать или промолчать (ответ потерян). */
export type BillingMode = 'ok' | 'fail' | 'reject' | 'silent';

/** Поведение участника по фазам: очередь режимов; пустая — `ok`. Полученные команды — в `received`. */
export interface BillingScript {
  readonly execute: BillingMode[];
  readonly compensate: BillingMode[];
  readonly received: SagaCommandMeta[];
}

export const newBillingScript = (): BillingScript => ({ execute: [], compensate: [], received: [] });

/**
 * Участник в той же базе: эффект шага — один на `stepKey` (inbox участника),
 * ответ — событием outbox в той же транзакции, что и эффект.
 */
export const billingParticipant =
  (script: BillingScript) =>
  (replica: SagaReplica): ReadonlyArray<readonly [string, OutboxDeliveryPort]> => {
    const port: OutboxDeliveryPort = async (delivery) => {
      const { meta } = parseSagaCommand(delivery.payload);
      script.received.push(meta);
      const mode = script[meta.phase].shift() ?? 'ok';
      if (mode === 'silent') {
        return;
      }
      await replica.transaction(async (tx) => {
        if (mode !== 'ok') {
          await replica.outbox.enqueue(
            tx,
            [
              sagaReplyMessage(
                sagaReplyOf(meta, { ok: false, error: `billing: ${mode}`, retryable: mode === 'fail' }),
              ),
            ],
            replica.clock.now,
          );
          return;
        }
        await replica.inbox.once(tx, 'billing', meta.stepKey, async () => {
          await recordEffect(tx, meta.sagaId, 'billing', meta.phase, meta.attempt);
        });
        // Дубль по stepKey (ручной повтор, повтор после таймаута) — тот же
        // исход ещё раз: эффект уже есть, а молчание оставило бы сагу ждать
        // ответа, который не придёт никогда.
        await replica.outbox.enqueue(
          tx,
          [sagaReplyMessage(sagaReplyOf(meta, { ok: true, data: { chargeId: `ch-${meta.sagaId}` } }))],
          replica.clock.now,
        );
      });
    };
    // Порт на тип: команда не той фазы под этим типом (возврат, отправленный
    // как списание) — отказ доставки, а не молча выполненный не тот шаг.
    const only =
      (phase: SagaCommandMeta['phase']): OutboxDeliveryPort =>
      async (delivery, ...rest) => {
        const { meta } = parseSagaCommand(delivery.payload);
        if (meta.phase !== phase) {
          throw new Error(`billing: команда фазы ${meta.phase} пришла типом для фазы ${phase}`);
        }
        await port(delivery, ...rest);
      };
    return [
      ['billing.charge', only('execute')],
      ['billing.refund', only('compensate')],
    ];
  };

/** Сага с удалённым шагом посередине: reserve (локальный) → charge (участник) → ship (локальный). */
export const paymentSaga = () =>
  defineSaga<{ orderId: string }>('payment', { version: 1 })
    .step('reserve', {
      execute: async (args) => {
        await recordEffect(args.tx, args.sagaId, 'reserve', 'execute', args.attempt);
        return { reservationId: `r-${args.context.orderId}` };
      },
      compensate: (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'compensate', args.attempt),
    })
    .remoteStep<{ chargeId: string }>('charge', {
      command: ({ context, stepKey }) => ({
        eventType: 'billing.charge',
        payload: { orderId: context.orderId, reservationId: context.reservationId, idempotencyKey: stepKey },
      }),
      compensation: ({ context }) => ({
        eventType: 'billing.refund',
        payload: { chargeId: context.chargeId ?? null },
      }),
      timeoutMs: 30_000,
      retry: { maxAttempts: 2, backoffMs: 1_000 },
      compensationRetry: { maxAttempts: 2, backoffMs: 1_000 },
    })
    .step('ship', {
      execute: async (args) => {
        await recordEffect(args.tx, args.sagaId, 'ship', 'execute', args.attempt);
        return { shipped: args.context.chargeId };
      },
    })
    .build();
