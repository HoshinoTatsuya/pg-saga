import {
  OUTBOX_FAIRNESS,
  OUTBOX_ORDERING,
  type OutboxDeliveryPort,
  type OutboxFairness,
} from '@hoshinotatsuya/outbox';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { sagaReplyOf } from '../remote/saga-reply-of';
import { billingParticipant, newBillingScript } from '../testing/billing-participant';
import {
  createReplica,
  effectsOf,
  instanceOf,
  journalOf,
  pump,
  recordEffect,
  type SagaReplica,
  START,
  type TestClock,
} from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';

/**
 * outbox `ordering: aggregate`: команда участнику, который лежит (повторы
 * релея до DEAD — сутки), не держит ни команды самой саги — компенсации после
 * таймера и после отмены, — ни команды ДРУГИМ участникам того же экземпляра.
 * Между собой команды одного шага (одному участнику) идут по порядку: возврат
 * не обгоняет списание.
 */
describeDb.each([
  ['захват по created_at', undefined],
  // Справедливый захват (по кругу между aggregate_type) не вправе ломать порядок внутри агрегата.
  ['справедливый захват по aggregate_type', OUTBOX_FAIRNESS.aggregateType],
] as ReadonlyArray<readonly [string, OutboxFairness | undefined]>)(
  'SagaOrchestrator: FIFO по агрегату и команды участникам (%s)',
  (_label, fairness) => {
    const fifo = { ordering: OUTBOX_ORDERING.aggregate, ...(fairness === undefined ? {} : { fairness }) };
    let database: TestDatabase;
    let clock: TestClock;

    beforeAll(async () => {
      database = await createTestDatabase();
      await createSagaSchema(database.pool);
    });

    afterAll(async () => {
      await database?.drop();
    });

    beforeEach(async () => {
      await truncateSagaSchema(database.pool);
      clock = { now: new Date(START) };
    });

    /** reserve (локальный, с компенсацией) → charge (удалённый, без повторов). */
    const saga = (name: string, timeoutMs: number, refund: boolean) =>
      defineSaga<{ orderId: string }>(name, { version: 1 })
        .step('reserve', {
          execute: async (args) => {
            await recordEffect(args.tx, args.sagaId, 'reserve', 'execute', args.attempt);
          },
          compensate: (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'compensate', args.attempt),
        })
        .remoteStep('charge', {
          command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
          ...(refund ? { compensation: () => ({ eventType: 'billing.refund', payload: {} }) } : {}),
          timeoutMs,
          retry: { maxAttempts: 1 },
        })
        .build();

    /** Участник лежит: каждая доставка — отказ (релей повторяет с паузой 1–5 с). */
    const down: OutboxDeliveryPort = async () => {
      throw new Error('billing лежит');
    };

    /** Секунда за секундой: что созрело — доставляется. */
    const seconds = async (replica: SagaReplica, count: number): Promise<void> => {
      for (let second = 0; second < count; second += 1) {
        for (let pass = 0; pass < 4; pass += 1) {
          await replica.relay.runOnce();
        }
        clock.now = new Date(clock.now.getTime() + 1_000);
      }
    };

    it('режим захвата набора действительно тот, что назван (иначе вариант ничего не проверял бы)', () => {
      const replica = createReplica({ pool: database.pool, clock, definitions: [], ...fifo });
      expect({ ordering: replica.outbox.config.ordering, fairness: replica.outbox.config.fairness }).toEqual({
        ordering: OUTBOX_ORDERING.aggregate,
        fairness: fairness ?? null,
      });
    });

    it('таймер ответа лежащему участнику: компенсации — сразу после таймера, а не после DEAD команды участника', async () => {
      const definition = saga('fifo-timer', 2_000, false);
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: () => [['billing.charge', down]],
      });
      await replica.start(definition, 'f-timer', { orderId: '1' });
      await seconds(replica, 30);

      const shown = await replica.admin.show('f-timer');
      const at = (event: string, step: string): number[] =>
        (shown?.journal ?? [])
          .filter((entry) => entry.event === event && entry.step === step)
          .map((entry) => entry.at.getTime() - START.getTime());
      const [timedOut] = at('step-timed-out', 'charge');
      const [compensated] = at('step-compensated', 'reserve');
      expect({
        status: shown?.instance.status,
        effects: await effectsOf(database.pool, 'f-timer'),
        // Компенсация — в ту же «секунду», что таймер: команда саги не стоит за
        // командой участника, которую релей всё ещё повторяет.
        delayMs: (compensated ?? Number.NaN) - (timedOut ?? Number.NaN),
      }).toEqual({
        status: SAGA_STATUS.compensated,
        effects: ['reserve:execute', 'reserve:compensate'],
        delayMs: 0,
      });
      // Выборка не выродилась: команда участнику всё это время на повторах.
      const { rows } = await database.pool.query<{ status: string; attempts: number }>(
        `SELECT status, attempts FROM ${TEST_TABLES.outbox} WHERE event_type = 'billing.charge'`,
      );
      expect(rows.map((row) => ({ status: row.status, retried: row.attempts > 3 }))).toEqual([
        { status: 'pending', retried: true },
      ]);
    });

    it('cancel при лежащем участнике: компенсации сразу, без сдвига часов', async () => {
      const definition = saga('fifo-cancel', 3_600_000, false);
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: () => [['billing.charge', down]],
      });
      await replica.start(definition, 'f-cancel', { orderId: '1' });
      for (let pass = 0; pass < 5; pass += 1) {
        await replica.relay.runOnce();
      }
      expect(await journalOf(replica, 'f-cancel')).toContain('step-dispatched:charge');
      await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'f-cancel', 'клиент передумал'));
      for (let pass = 0; pass < 5; pass += 1) {
        await replica.relay.runOnce();
      }

      expect({
        status: (await instanceOf(replica, 'f-cancel')).status,
        effects: await effectsOf(database.pool, 'f-cancel'),
      }).toEqual({ status: SAGA_STATUS.compensated, effects: ['reserve:execute', 'reserve:compensate'] });
    });

    it('команды участникам одного экземпляра — по порядку: возврат после отмены не обгоняет ждущее списание', async () => {
      const definition = saga('fifo-order', 3_600_000, true);
      const phases: string[] = [];
      let failures = 3;
      const port: OutboxDeliveryPort = async (delivery) => {
        const { meta } = parseSagaCommand(delivery.payload);
        if (meta.phase === 'execute' && failures > 0) {
          failures -= 1;
          throw new Error('billing лежит');
        }
        phases.push(meta.phase);
      };
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: () => [
          ['billing.charge', port],
          ['billing.refund', port],
        ],
      });
      await replica.start(definition, 'f-order', { orderId: '1' });
      for (let pass = 0; pass < 5; pass += 1) {
        await replica.relay.runOnce();
      }
      await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'f-order', 'клиент передумал'));
      await seconds(replica, 30);

      expect({ phases, failures }).toEqual({ phases: ['execute', 'compensate'], failures: 0 });
    });

    it('списание ушло с реплики, чьи часы впереди на 5 мин, ещё не доставлено: возврат с реплики с верными часами не обгоняет его', async () => {
      // FIFO outbox упорядочивает агрегат по (created_at, id): возврат, записанный
      // по отстающим часам раньше списания, встал бы в очереди перед ним.
      const definition = saga('fifo-skew', 3_600_000, true);
      const phases: string[] = [];
      let failures = 3;
      const port: OutboxDeliveryPort = async (delivery) => {
        const { meta } = parseSagaCommand(delivery.payload);
        if (meta.phase === 'execute' && failures > 0) {
          failures -= 1;
          throw new Error('billing лежит');
        }
        phases.push(meta.phase);
      };
      const participants = (): ReadonlyArray<readonly [string, OutboxDeliveryPort]> => [
        ['billing.charge', port],
        ['billing.refund', port],
      ];
      const fastClock: TestClock = { now: new Date(START.getTime() + 5 * 60_000) };
      const fast = createReplica({
        pool: database.pool,
        clock: fastClock,
        definitions: [definition],
        ...fifo,
        participants,
      });
      const slow = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants,
      });
      await slow.start(definition, 'f-skew', { orderId: '1' });
      // reserve — верная реплика; команду списания пишет реплика с часами впереди.
      await slow.relay.runOnce();
      await fast.relay.runOnce();
      expect(await journalOf(slow, 'f-skew')).toContain('step-dispatched:charge');
      await slow.transaction((tx) => slow.orchestrator.cancel(tx, 'f-skew', 'клиент передумал'));
      // Дальше доставляет только верная реплика: её часы идут до +10 мин.
      for (let step = 0; step < 40; step += 1) {
        for (let pass = 0; pass < 4; pass += 1) {
          await slow.relay.runOnce();
        }
        clock.now = new Date(clock.now.getTime() + 15_000);
      }

      // Участник не отвечает: экземпляр ждёт ответа на возврат (compensating).
      expect({ phases, failures, status: (await instanceOf(slow, 'f-skew')).status }).toEqual({
        phases: ['execute', 'compensate'],
        failures: 0,
        status: SAGA_STATUS.compensating,
      });
    });

    it('списание и возврат записаны в одну миллисекунду (неподвижные часы): возврат встаёт строго после — порядок не решает id', async () => {
      const definition = saga('fifo-same-ms', 3_600_000, true);
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: () => [
          ['billing.charge', down],
          ['billing.refund', down],
        ],
      });
      await replica.start(definition, 'f-same', { orderId: '1' });
      for (let pass = 0; pass < 5; pass += 1) {
        await replica.relay.runOnce();
      }
      await replica.transaction((tx) => replica.orchestrator.cancel(tx, 'f-same', 'клиент передумал'));
      for (let pass = 0; pass < 5; pass += 1) {
        await replica.relay.runOnce();
      }
      const { rows } = await database.pool.query<{ event_type: string; created_ms: number }>(
        `SELECT event_type, (EXTRACT(EPOCH FROM created_at) * 1000)::float8 - $1::float8 AS created_ms
         FROM ${TEST_TABLES.outbox} WHERE event_type IN ('billing.charge', 'billing.refund')
        ORDER BY created_at, id`,
        [START.getTime()],
      );
      // Часы не двигались: списание — в START, возврат — на миллисекунду позже.
      expect(rows).toEqual([
        { event_type: 'billing.charge', created_ms: 0 },
        { event_type: 'billing.refund', created_ms: 1 },
      ]);
    });

    it('два участника: лежащий B (шаг без компенсации) не держит компенсацию у ЖИВОГО A — сага compensated сразу после таймера B', async () => {
      const definition = defineSaga<{ orderId: string }>('fifo-two', { version: 1 })
        .remoteStep<{ reservationId: string }>('reserve', {
          command: ({ stepKey }) => ({ eventType: 'inventory.reserve', payload: { stepKey } }),
          compensation: ({ executeStepKey }) => ({
            eventType: 'inventory.release',
            payload: { executeStepKey },
          }),
          timeoutMs: 2_000,
          retry: { maxAttempts: 1 },
          compensationRetry: { maxAttempts: 3, backoffMs: 1_000, maxBackoffMs: 2_000 },
        })
        // «Известить»: компенсации нет, участник лежит.
        .remoteStep('notify', {
          command: ({ stepKey }) => ({ eventType: 'mailer.notify', payload: { stepKey } }),
          timeoutMs: 2_000,
          retry: { maxAttempts: 1 },
        })
        .build();
      const inventory: string[] = [];
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: (r) => {
          const live: OutboxDeliveryPort = async (delivery) => {
            const { meta } = parseSagaCommand(delivery.payload);
            inventory.push(meta.phase);
            await r.orchestrator.handleReply(
              sagaReplyOf(meta, { ok: true, data: meta.phase === 'execute' ? { reservationId: 'R1' } : {} }),
            );
          };
          return [
            ['inventory.reserve', live],
            ['inventory.release', live],
            ['mailer.notify', down],
          ];
        },
      });
      await replica.start(definition, 'f-two', { orderId: '1' });
      await seconds(replica, 30);

      const shown = await replica.admin.show('f-two');
      const at = (event: string, step: string): number[] =>
        (shown?.journal ?? [])
          .filter((entry) => entry.event === event && entry.step === step)
          .map((entry) => entry.at.getTime() - START.getTime());
      expect({
        status: shown?.instance.status,
        inventory,
        compensationFailures: at('compensation-failed', 'reserve').length,
        // Компенсация A — в ту же «секунду», что таймер B.
        delayMs:
          (at('step-compensated', 'reserve')[0] ?? Number.NaN) -
          (at('step-timed-out', 'notify')[0] ?? Number.NaN),
      }).toEqual({
        status: SAGA_STATUS.compensated,
        inventory: ['execute', 'compensate'],
        compensationFailures: 0,
        delayMs: 0,
      });
      // Выборка не выродилась: команда лежащему B всё это время на повторах, в
      // своём агрегате — не в агрегате команд A.
      const { rows } = await database.pool.query<{
        event_type: string;
        aggregate_type: string;
        retried: boolean;
      }>(
        `SELECT DISTINCT event_type, aggregate_type, (status = 'pending' AND attempts > 3) AS retried
         FROM ${TEST_TABLES.outbox} WHERE event_type IN ('inventory.reserve', 'inventory.release', 'mailer.notify')
        ORDER BY 1`,
      );
      expect(rows).toEqual([
        { event_type: 'inventory.release', aggregate_type: 'saga-remote:0', retried: false },
        { event_type: 'inventory.reserve', aggregate_type: 'saga-remote:0', retried: false },
        { event_type: 'mailer.notify', aggregate_type: 'saga-remote:1', retried: true },
      ]);
    });

    it('свой aggregateType «saga» у команды участнику — окончательный отказ шага (держал бы команды самой саги)', async () => {
      const definition = defineSaga<{ orderId: string }>('fifo-own-saga', { version: 1 })
        .step('reserve', {
          execute: async (args) => {
            await recordEffect(args.tx, args.sagaId, 'reserve', 'execute', args.attempt);
          },
          compensate: (args) => recordEffect(args.tx, args.sagaId, 'reserve', 'compensate', args.attempt),
        })
        .remoteStep('charge', {
          command: () => ({ eventType: 'billing.charge', payload: {}, aggregateType: 'saga' }),
          timeoutMs: 60_000,
        })
        .build();
      const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
      await replica.start(definition, 'f-own', { orderId: '1' });
      await pump(clock, [replica]);
      expect({
        status: (await instanceOf(replica, 'f-own')).status,
        effects: await effectsOf(database.pool, 'f-own'),
        journal: (await journalOf(replica, 'f-own')).filter((event) => event.startsWith('step-failed')),
      }).toEqual({
        status: SAGA_STATUS.compensated,
        effects: ['reserve:execute', 'reserve:compensate'],
        journal: ['step-failed:charge'],
      });
    });

    it('двойник: свой aggregateType участника (не «saga») — принят, команда в нём', async () => {
      const billing = newBillingScript();
      const definition = defineSaga<{ orderId: string }>('fifo-own-type', { version: 1 })
        .remoteStep('charge', {
          command: () => ({ eventType: 'billing.charge', payload: {}, aggregateType: 'billing' }),
          timeoutMs: 60_000,
        })
        .build();
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: billingParticipant(billing),
      });
      await replica.start(definition, 'f-type', { orderId: '1' });
      await pump(clock, [replica]);
      const { rows } = await database.pool.query<{ aggregate_type: string; aggregate_id: string }>(
        `SELECT aggregate_type, aggregate_id FROM ${TEST_TABLES.outbox} WHERE event_type = 'billing.charge'`,
      );
      expect({ status: (await instanceOf(replica, 'f-type')).status, rows }).toEqual({
        status: SAGA_STATUS.completed,
        rows: [{ aggregate_type: 'billing', aggregate_id: 'f-type' }],
      });
    });

    it('агрегат каждого вида сообщения саги: команда, отложенная команда, таймер, команда участнику, ответ', async () => {
      const billing = newBillingScript();
      billing.execute.push('fail');
      const definition = defineSaga<{ orderId: string }>('fifo-kinds', { version: 1 })
        .step('reserve', { execute: async () => ({ r: 1 }) })
        .remoteStep('charge', {
          command: () => ({ eventType: 'billing.charge', payload: {} }),
          timeoutMs: 60_000,
          retry: { maxAttempts: 2, backoffMs: 1_000 },
        })
        .build();
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...fifo,
        participants: billingParticipant(billing),
      });
      await replica.start(definition, 'f-kinds', { orderId: '1' });
      await pump(clock, [replica]);
      expect((await instanceOf(replica, 'f-kinds')).status).toBe(SAGA_STATUS.completed);

      const { rows } = await database.pool.query<{ kind: string; aggregate_type: string; own: boolean }>(
        `SELECT DISTINCT CASE WHEN event_type = 'saga.command' AND next_attempt_at > created_at THEN 'deferred'
                            ELSE event_type END AS kind,
                       aggregate_type, aggregate_id = id::text AS own
         FROM ${TEST_TABLES.outbox} ORDER BY 1, 2`,
      );
      expect(rows).toEqual([
        { kind: 'billing.charge', aggregate_type: 'saga-remote:1', own: false },
        { kind: 'deferred', aggregate_type: 'saga-deferred', own: true },
        { kind: 'saga.command', aggregate_type: 'saga', own: false },
        { kind: 'saga.reply', aggregate_type: 'saga-reply', own: true },
        { kind: 'saga.timer', aggregate_type: 'saga-timer', own: true },
      ]);
    });
  },
);
