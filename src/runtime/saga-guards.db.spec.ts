import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { Pool } from 'pg';

import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaDefinitionUnavailableError } from '../errors/saga-definition-unavailable.error';
import { sagaReplyOf } from '../remote/saga-reply-of';
import { billingParticipant, newBillingScript, paymentSaga } from '../testing/billing-participant';
import {
  createReplica,
  instanceOf,
  journalOf,
  pump,
  recordEffect,
  START,
  type TestClock,
} from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  rejectionOf,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import { newScript, orderSaga } from '../testing/test-sagas';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';
import { sagaMessageId } from './saga-message-id';
import { sagaTransactionRunner } from './saga-transaction-runner';

/**
 * Гарантии, которые прежде держались только кодом: каждое поле адреса
 * сообщения отсеивает чужое сообщение ПО ОТДЕЛЬНОСТИ, брошенная релеем
 * доставка не пишется отказом, шаг, игнорирующий сигнал, всё равно снимается
 * по таймауту, укороченное определение под той же версией — отказ.
 */
describeDb('SagaOrchestrator: защитные проверки', () => {
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

  const commandOf = async (sagaId: string): Promise<Record<string, unknown>> => {
    const { rows } = await database.pool.query<{ payload: Record<string, unknown> }>(
      `SELECT payload FROM ${TEST_TABLES.outbox}
        WHERE event_type = 'saga.command' AND status = 'pending' AND aggregate_id = $1`,
      [sagaId],
    );
    const payload = rows[0]?.payload;
    if (payload === undefined) {
      throw new Error('нет ждущей команды');
    }
    return payload;
  };

  /**
   * id сообщения — от его адреса и воплощения экземпляра, как его выдал бы
   * оркестратор: иначе сообщение отсеет сверка id, и проверка поля по
   * отдельности ничего бы не доказала (два рубежа маскируют друг друга).
   */
  const idOf = async (
    kind: 'command' | 'remote',
    replica: ReturnType<typeof createReplica>,
    address: {
      sagaId: string;
      generation: number;
      phase: 'execute' | 'compensate';
      stepIndex: number;
      attempt: number;
    },
  ): Promise<string> =>
    sagaMessageId[kind]({
      ...address,
      incarnation: (await instanceOf(replica, address.sagaId)).incarnation,
    });

  it.each([
    ['поколение', { generation: 1 }],
    ['фаза', { phase: 'compensate' }],
    ['шаг', { stepIndex: 1 }],
    ['попытка', { attempt: 2 }],
    ['воплощение (id прежнего экземпляра того же id)', {}],
  ])('команда с чужим полем «%s» — stale, экземпляр не тронут', async (field, patch) => {
    const definition = orderSaga(newScript());
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'g-cmd', { orderId: '1' });
    const address = { ...(await commandOf('g-cmd')), ...patch } as Parameters<typeof idOf>[2];
    const command = {
      ...address,
      commandId: field.startsWith('воплощение')
        ? sagaMessageId.command({ ...address, incarnation: START.getTime() })
        : await idOf('command', replica, address),
    };

    expect(await replica.orchestrator.handle('saga.command', command)).toBe(SAGA_HANDLED.stale);
    expect(await journalOf(replica, 'g-cmd')).toEqual(['saga-started']);
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it.each([
    ['поколение', { generation: 1 }],
    ['фаза', { phase: 'compensate' as const }],
    ['шаг (пройденный)', { stepIndex: 0 }],
    ['попытка (следующая)', { attempt: 2 }],
  ])('ответ с чужим полем «%s» — stale, шаг не сдвинут', async (_field, patch) => {
    const billing = newBillingScript();
    billing.execute.push('silent');
    const definition = paymentSaga();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'g-reply', { orderId: '1' });
    for (let round = 0; round < 5; round += 1) {
      await replica.relay.runOnce();
    }
    const sent = billing.received.at(-1);
    if (sent === undefined) {
      throw new Error('участник не получил команду');
    }
    const patched = { ...sent, ...patch };
    const meta: SagaCommandMeta = { ...patched, commandId: await idOf('remote', replica, patched) };

    expect(await replica.orchestrator.handleReply(sagaReplyOf(meta, { ok: true }))).toBe(SAGA_HANDLED.stale);
    expect((await instanceOf(replica, 'g-reply')).stepIndex).toBe(1);
    // Двойник: тот же ответ с полями своей попытки — применяется (поле решает само).
    expect(await replica.orchestrator.handleReply(sagaReplyOf(sent, { ok: true }))).toBe(
      SAGA_HANDLED.applied,
    );
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('доставку бросил релей (сигнал доставки) — наружу без записи отказа: повторит релей', async () => {
    const script = newScript();
    script.execute.set('reserve', 'hang');
    const definition = orderSaga(script);
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'g-abort', { orderId: '1' });
    const delivery = new AbortController();
    const handled = replica.orchestrator.handle('saga.command', await commandOf('g-abort'), delivery.signal);
    setTimeout(() => delivery.abort(new Error('релей: потолок доставки')), 50);

    expect(String(await rejectionOf(handled))).toContain('потолок доставки');
    const instance = await instanceOf(replica, 'g-abort');
    expect({ attempt: instance.attempt, journal: await journalOf(replica, 'g-abort') }).toEqual({
      attempt: 1,
      journal: ['saga-started'],
    });
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('шаг, не слушающий сигнал, всё равно снимается по timeoutMs (отказ по таймауту, а не вечное ожидание)', async () => {
    let calls = 0;
    const deaf = defineSaga<{ orderId: string }>('deaf', { version: 1 })
      .step('wait', {
        execute: async () => {
          calls += 1;
          if (calls === 1) {
            await new Promise<never>(() => undefined);
          }
        },
        timeoutMs: 100,
        retry: { maxAttempts: 2, backoffMs: 1 },
      })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [deaf] });
    await replica.start(deaf, 'g-deaf', { orderId: '1' });
    await pump(clock, [replica]);

    expect({
      status: (await instanceOf(replica, 'g-deaf')).status,
      journal: await journalOf(replica, 'g-deaf'),
    }).toEqual({
      status: SAGA_STATUS.completed,
      journal: ['saga-started', 'step-timed-out:wait', 'step-completed:wait', 'saga-completed'],
    });
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it.each([
    ['раннер саги (transactionKeepingFailure)', undefined],
    // Свой раннер без защиты соединения: соединение после транзакции — в пуле,
    // и запрос брошенного шага ушёл бы мимо транзакции или в чужую.
    [
      'свой раннер (db.transaction drizzle)',
      (pool: Pool): SagaTransactionRunner => {
        const db = drizzle(pool);
        return (work) => db.transaction(work);
      },
    ],
  ] as ReadonlyArray<readonly [string, ((pool: Pool) => SagaTransactionRunner) | undefined]>)(
    'шаг, брошенный по таймауту (%s), пишет через tx после отказа — запрос отказан, эффекта нет, повтор доезжает',
    async (_label, runnerOf) => {
      let release = (): void => undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let abandoned: Promise<unknown> = Promise.resolve('шаг не запускался');
      const definition = defineSaga<{ orderId: string }>('abandoned', { version: 1 })
        .step('write', {
          execute: async ({ tx, sagaId, attempt }) => {
            if (attempt === 1) {
              // Глух к сигналу: ждёт своего, потом пишет через tx уже закрытой транзакции.
              abandoned = gate
                .then(() => recordEffect(tx, sagaId, 'write', 'execute', attempt))
                .then(
                  () => 'записал',
                  (error: unknown) => error,
                );
              await abandoned;
              return;
            }
            await recordEffect(tx, sagaId, 'write', 'execute', attempt);
          },
          timeoutMs: 100,
          retry: { maxAttempts: 2, backoffMs: 1 },
        })
        .build();
      const replica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        ...(runnerOf === undefined ? {} : { transaction: () => runnerOf(database.pool) }),
      });
      await replica.start(definition, 'g-abandoned', { orderId: '1' });
      await pump(clock, [replica]);
      release();
      const outcome = await abandoned;

      expect({
        outcome: outcome instanceof Error ? 'отказ' : outcome,
        effects: (
          await database.pool.query<{ attempt: number }>(
            `SELECT attempt FROM effects WHERE saga_id = 'g-abandoned' ORDER BY id`,
          )
        ).rows.map((row) => row.attempt),
        journal: await journalOf(replica, 'g-abandoned'),
      }).toEqual({
        outcome: 'отказ',
        effects: [2],
        journal: ['saga-started', 'step-timed-out:write', 'step-completed:write', 'saga-completed'],
      });
      await replica.relay.stop({ deadlineMs: 1_000 });
    },
  );

  it('определение под той же версией без последнего шага — отказ с причиной, а не «экземпляр доехал»', async () => {
    const stepsOf = (names: readonly string[]) => {
      let builder = defineSaga<{ orderId: string }>('short', { version: 1 });
      for (const name of names) {
        builder = builder.step(name, {
          execute: async (args) => {
            await recordEffect(args.tx, args.sagaId, name, 'execute', args.attempt);
          },
        });
      }
      return builder.build();
    };
    const full = stepsOf(['a', 'b', 'c']);
    const old = createReplica({ pool: database.pool, clock, definitions: [full] });
    await old.start(full, 'g-short', { orderId: '1' });

    const cut = createReplica({ pool: database.pool, clock, definitions: [stepsOf(['a', 'b'])] });
    const refused = await rejectionOf(cut.orchestrator.handle('saga.command', await commandOf('g-short')));

    expect(refused).toBeInstanceOf(SagaDefinitionUnavailableError);
    expect(String(refused)).toContain('разошлись');
    await old.relay.stop({ deadlineMs: 1_000 });
    await cut.relay.stop({ deadlineMs: 1_000 });
  });

  it('шаг проглотил ошибку SQL и сразу вернулся — отказ шага с причиной, а не повторы доставки до DEAD', async () => {
    let calls = 0;
    const swallowing = defineSaga<{ orderId: string }>('swallow', { version: 1 })
      .step('only', {
        execute: async ({ tx }) => {
          calls += 1;
          if (calls === 1) {
            try {
              await tx.execute(sql`SELECT 1 / 0`);
            } catch {
              // «Обработал» и вернулся без единого запроса после — транзакция прервана.
            }
          }
        },
        retry: { maxAttempts: 2, backoffMs: 1 },
      })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [swallowing] });
    await replica.start(swallowing, 'g-swallow', { orderId: '1' });
    await pump(clock, [replica]);

    const shown = await replica.admin.show('g-swallow');
    expect({
      status: shown?.instance.status,
      failed: shown?.journal.find((entry) => entry.event === 'step-failed')?.detail,
    }).toEqual({
      status: SAGA_STATUS.completed,
      failed: expect.stringContaining('проглотил ошибку SQL'),
    });
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('шаг получает замороженную копию контекста: изменение на месте не попадает в сохранённый контекст', async () => {
    let threw = false;
    const mutating = defineSaga<{ orderId: string }>('mutating', { version: 1 })
      .step('a', { execute: async () => ({ items: [1] }) })
      .step('b', {
        execute: async ({ context }) => {
          try {
            (context.items as number[]).push(2);
          } catch {
            threw = true;
          }
        },
      })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [mutating] });
    await replica.start(mutating, 'g-mutate', { orderId: '1' });
    await pump(clock, [replica]);

    expect({ threw, items: (await instanceOf(replica, 'g-mutate')).context.items }).toEqual({
      threw: true,
      items: [1],
    });
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('stepIndex вне шагов экземпляра (строку правили руками) — отказ с причиной, а не голый Error', async () => {
    const definition = orderSaga(newScript());
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'g-index', { orderId: '1' });
    const address = { ...(await commandOf('g-index')), stepIndex: 99 } as Parameters<typeof idOf>[2];
    const command = { ...address, commandId: await idOf('command', replica, address) };
    await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET step_index = 99 WHERE id = 'g-index'`);

    const refused = await rejectionOf(replica.orchestrator.handle('saga.command', command));
    expect(refused).toBeInstanceOf(SagaDefinitionUnavailableError);
    expect(String(refused)).toContain('вне шагов');
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('срезы: limit сверх потолка, окно stats из будущего, поколение сверх int4 — отказ до запроса и записи', async () => {
    const definition = orderSaga(newScript());
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await expect(replica.admin.list({ limit: 10_001 })).rejects.toThrow(SagaConfigError);
    await expect(replica.admin.stats(new Date(clock.now.getTime() + 1))).rejects.toThrow(SagaConfigError);

    const script = newScript();
    script.execute.set('reserve', 'reject');
    script.compensate.set('reserve', 'always');
    const stuckSaga = orderSaga(script);
    const other = createReplica({ pool: database.pool, clock, definitions: [stuckSaga] });
    await database.pool.query(`TRUNCATE ${TEST_TABLES.instances} CASCADE`);
    await other.start(stuckSaga, 'g-gen', { orderId: '1' });
    await database.pool.query(
      `UPDATE ${TEST_TABLES.instances} SET status = 'stuck', generation = 2147483647 WHERE id = 'g-gen'`,
    );
    await expect(other.admin.retry('g-gen')).rejects.toThrow(SagaConfigError);
    await replica.relay.stop({ deadlineMs: 1_000 });
    await other.relay.stop({ deadlineMs: 1_000 });
  });

  it('отметка inbox ставится по часам оркестратора, а не по настенным', async () => {
    const definition = orderSaga(newScript());
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'g-clock', { orderId: '1' });
    await pump(clock, [replica]);

    const { rows } = await database.pool.query<{ latest: Date }>(
      `SELECT max(processed_at) AS latest FROM ${TEST_TABLES.inbox}`,
    );
    // Часы теста стоят в START (+ паузы повтора); настенные — сегодня.
    expect(rows[0]?.latest.getTime()).toBeLessThanOrEqual(clock.now.getTime());
    expect(rows[0]?.latest.getTime()).toBeGreaterThanOrEqual(START.getTime());
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('sagaTransactionRunner: lockTimeoutMs — потолок блокировок внутри транзакции саги', async () => {
    const replica = createReplica({ pool: database.pool, clock, definitions: [] });
    const run = sagaTransactionRunner(replica.db, { lockTimeoutMs: 1_234 });
    const seen = await run(async (tx) => {
      const result = await tx.execute(sql`SELECT current_setting('lock_timeout') AS value`);
      return String((result.rows[0] as { value: string }).value);
    });

    expect(seen).toBe('1234ms');
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('ответ на ЛОКАЛЬНЫЙ шаг с id, которого экземпляр не выдавал, — stale, а не «вид шага сменили»', async () => {
    const definition = orderSaga(newScript());
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'g-local', { orderId: '1' });
    const reply = {
      commandId: '6b0b9b1e-0000-4000-8000-000000000000',
      sagaId: 'g-local',
      generation: 0,
      phase: 'execute',
      stepIndex: 0,
      attempt: 1,
      outcome: 'ok',
    };
    expect(await replica.orchestrator.handleReply(reply)).toBe(SAGA_HANDLED.stale);
    expect(await journalOf(replica, 'g-local')).toEqual(['saga-started']);
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('успех КОМПЕНСАЦИИ с данными, которых не сохранить (NUL), — компенсация выполнена: данные компенсации в контекст не идут', async () => {
    const billing = newBillingScript();
    billing.execute.push('silent', 'silent');
    billing.compensate.push('silent');
    const definition = paymentSaga();
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'g-comp-data', { orderId: '1' });
    const passes = async (count: number): Promise<void> => {
      for (let pass = 0; pass < count; pass += 1) {
        await replica.relay.runOnce();
      }
    };
    // Попытка 1 молчит → таймер → пауза → попытка 2 молчит → таймер → возврат (молчит).
    await passes(6);
    clock.now = new Date(clock.now.getTime() + 30_001);
    await passes(3);
    clock.now = new Date(clock.now.getTime() + 1_000);
    await passes(4);
    clock.now = new Date(clock.now.getTime() + 30_001);
    await passes(6);
    expect((await instanceOf(replica, 'g-comp-data')).status).toBe(SAGA_STATUS.compensating);
    const refund = billing.received.find((meta) => meta.phase === 'compensate');
    if (refund === undefined) {
      throw new Error('участник не получил возврат');
    }
    const before = (await instanceOf(replica, 'g-comp-data')).context;
    expect(
      await replica.orchestrator.handleReply(
        sagaReplyOf(refund, { ok: true, data: { note: 'a\u0000b', extra: 1 } }),
      ),
    ).toBe(SAGA_HANDLED.applied);
    const after = await instanceOf(replica, 'g-comp-data');
    expect({
      stepIndex: after.stepIndex,
      context: after.context,
      journal: (await journalOf(replica, 'g-comp-data')).at(-1),
    }).toEqual({
      stepIndex: 0,
      context: before,
      journal: 'step-compensated:charge',
    });
    await replica.relay.stop({ deadlineMs: 1_000 });
  });

  it('SQL шага отменён сервером (57014) раньше таймера процесса — отказ по ТАЙМАУТУ, а не обычный отказ', async () => {
    const definition = defineSaga<{ orderId: string }>('server-timeout', { version: 1 })
      .step('a', {
        execute: async ({ tx }) => {
          // Свой потолок короче таймера шага: сервер снимет запрос первым.
          await tx.execute(sql`SELECT set_config('statement_timeout', '50ms', true)`);
          await tx.execute(sql`SELECT pg_sleep(2)`);
        },
        timeoutMs: 10_000,
        retry: { maxAttempts: 1 },
      })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'g-57014', { orderId: '1' });
    await pump(clock, [replica]);
    expect(await journalOf(replica, 'g-57014')).toEqual([
      'saga-started',
      'step-timed-out:a',
      'saga-compensated',
    ]);
    await replica.relay.stop({ deadlineMs: 1_000 });
  });
});
