import type { OutboxDeliveryPort } from '@hoshinotatsuya/outbox';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { sagaReplyMessage } from '../remote/saga-reply-message';
import { sagaReplyOf } from '../remote/saga-reply-of';
import {
  createReplica,
  instanceOf,
  journalOf,
  pump,
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

/** Обрезка сообщения посреди эмодзи — непарная половина суррогатной пары. */
const CUT = 'отказ по вводу 😀'.slice(0, -1);

/**
 * Отказ с причиной, которую `String()` не берёт, и текст с непарным суррогатом:
 * отказ ЗАПИСЫВАЕТСЯ тем же проходом, а не повторяется до DEAD (экземпляр вечно
 * running), и ответ участника доходит, а не роняет его транзакцию 22P02.
 */
describeDb('Текст отказа: непреобразуемые значения и непарный суррогат', () => {
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

  const commandAttempts = async (): Promise<Array<{ status: string; attempts: number }>> =>
    (
      await database.pool.query<{ status: string; attempts: number }>(
        `SELECT status, attempts FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.command' ORDER BY created_at`,
      )
    ).rows;

  it.each([
    ['объект без прототипа', () => Object.create(null) as unknown],
    ['Error с непарным суррогатом', () => new Error(CUT)],
  ])('локальный шаг бросил %s — отказ записан с первой доставки', async (_name, thrown) => {
    const definition = defineSaga<{ n: number }>('odd-throw', { version: 1 })
      .step('a', {
        execute: async () => {
          throw thrown();
        },
        retry: { maxAttempts: 1 },
      })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'o-1', { n: 1 });
    await replica.relay.runOnce();

    expect(await journalOf(replica, 'o-1')).toEqual(['saga-started', 'step-failed:a', 'saga-compensated']);
    expect(await commandAttempts()).toEqual([{ status: 'published', attempts: 0 }]);
    const instance = await instanceOf(replica, 'o-1');
    expect({ status: instance.status, wellFormed: instance.failure?.isWellFormed() }).toEqual({
      status: SAGA_STATUS.compensated,
      wellFormed: true,
    });
  });

  it('участник отвечает отказом с непарным суррогатом в тексте — ответ записан, сага получает step-failed', async () => {
    const definition = defineSaga<{ n: number }>('odd-reply', { version: 1 })
      .remoteStep('charge', {
        command: () => ({ eventType: 'billing.charge', payload: {} }),
        timeoutMs: 60_000,
        retry: { maxAttempts: 1 },
      })
      .build();
    const participantErrors: string[] = [];
    const port =
      (replica: SagaReplica): OutboxDeliveryPort =>
      async (delivery) => {
        const { meta } = parseSagaCommand(delivery.payload);
        try {
          await replica.transaction((tx) =>
            replica.outbox.enqueue(
              tx,
              [sagaReplyMessage(sagaReplyOf(meta, { ok: false, error: new Error(CUT), retryable: false }))],
              clock.now,
            ),
          );
        } catch (error) {
          participantErrors.push(String((error as Error).message));
          throw error;
        }
      };
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: (self) => [['billing.charge', port(self)]],
    });
    await replica.start(definition, 'r-1', { n: 1 });
    await pump(clock, [replica]);

    expect(participantErrors).toEqual([]);
    expect(await journalOf(replica, 'r-1')).toEqual([
      'saga-started',
      'step-dispatched:charge',
      'step-failed:charge',
      'saga-compensated',
    ]);
    const instance = await instanceOf(replica, 'r-1');
    expect(instance.failure).toContain('отказ по вводу �');
  });

  it('локальный шаг вернул строку с непарным суррогатом — отказ шага без повторов, а не 22P02 на записи перехода до DEAD', async () => {
    const definition = defineSaga<{ n: number }>('odd-output', { version: 1 })
      .step('a', { execute: async () => ({ note: CUT }), retry: { maxAttempts: 3 } })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'u-1', { n: 1 });
    await replica.relay.runOnce();

    expect(await journalOf(replica, 'u-1')).toEqual(['saga-started', 'step-failed:a', 'saga-compensated']);
    expect(await commandAttempts()).toEqual([{ status: 'published', attempts: 0 }]);
  });
});
