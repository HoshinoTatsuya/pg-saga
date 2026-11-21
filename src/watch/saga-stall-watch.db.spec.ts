import { sql } from 'drizzle-orm';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaMessageId } from '../runtime/saga-message-id';
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
import type { SagaMetrics } from '../types/saga-metrics.type';
import { SagaStallWatch } from './saga-stall-watch';

const MINUTE = 60_000;
const TABLES = { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal };

/**
 * Сторож: экземпляр, чьё ОЖИДАЕМОЕ сообщение ушло в DEAD outbox, — `blocked`
 * (журнал, хук, ручной повтор без порога простоя); DEAD прошлой попытки или
 * поколения живой экземпляр не останавливает; неподвижный без DEAD — `stalled`,
 * один раз на эпизод и без повтора.
 */
describeDb('SagaStallWatch', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let blockedEvents: Array<Parameters<NonNullable<SagaMetrics['blocked']>>[0]>;
  let stalledEvents: string[];
  /** Шаги, которые обрывают СВОЁ соединение на каждой доставке (отказ инфраструктуры). */
  let broken: Set<string>;

  /** Сага из двух локальных шагов; шаг из `broken` обрывает своё соединение — доставка отказывает. */
  const twoSteps = defineSaga<{ n: number }>('dead', { version: 1 })
    .step('a', {
      execute: async ({ tx }) => {
        if (broken.has('a')) {
          await tx.execute(sql`SELECT pg_terminate_backend(pg_backend_pid())`);
        }
        return { a: 1 };
      },
      compensate: async ({ tx }) => {
        if (broken.has('a:compensate')) {
          await tx.execute(sql`SELECT pg_terminate_backend(pg_backend_pid())`);
        }
      },
    })
    .step('b', {
      execute: async () => {
        if (broken.has('b:reject')) {
          throw new Error('b отказал');
        }
        return { b: 1 };
      },
      retry: { maxAttempts: 1 },
    })
    .build();

  const replicaFor = (): SagaReplica =>
    createReplica({
      pool: database.newPool(4),
      clock,
      definitions: [twoSteps],
      outboxLimits: { maxAttempts: 3, deadAfterMs: 0 },
    });

  const watchOf = (replica: SagaReplica): SagaStallWatch =>
    new SagaStallWatch({
      db: replica.db,
      transaction: replica.transaction,
      outbox: replica.outbox,
      tables: TABLES,
      stalledAfterMs: 30 * MINUTE,
      clock: () => clock.now,
      metrics: {
        blocked: (event) => blockedEvents.push(event),
        stalled: (event) => stalledEvents.push(`${event.sagaId}:${event.step}:${event.status}`),
      },
    });

  const outboxStatusOf = async (id: string): Promise<string | undefined> =>
    (
      await database.pool.query<{ status: string }>(
        `SELECT status FROM ${TEST_TABLES.outbox} WHERE id = $1`,
        [id],
      )
    ).rows[0]?.status;

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
    blockedEvents = [];
    stalledEvents = [];
    broken = new Set();
  });

  it('шаг отказывает на каждой доставке до maxAttempts: один проход сторожа — blocked; ручной повтор без порога доводит сагу', async () => {
    const replica = replicaFor();
    broken.add('a');
    await replica.start(twoSteps, 'd-1', { n: 1 });
    await pump(clock, [replica]);
    const commandId = sagaMessageId.command({
      sagaId: 'd-1',
      incarnation: (await instanceOf(replica, 'd-1')).incarnation,
      generation: 0,
      phase: 'execute',
      stepIndex: 0,
      attempt: 1,
    });
    // Выборка не выродилась: доставка ушла в DEAD, экземпляр о ней не знает.
    expect({
      outbox: await outboxStatusOf(commandId),
      status: (await instanceOf(replica, 'd-1')).status,
    }).toEqual({
      outbox: 'dead',
      status: SAGA_STATUS.running,
    });

    const report = await watchOf(replica).runOnce();

    const instance = await instanceOf(replica, 'd-1');
    expect({
      blocked: report.blocked,
      status: instance.status,
      lastError: instance.lastError !== null,
    }).toEqual({
      blocked: ['d-1'],
      status: SAGA_STATUS.blocked,
      lastError: true,
    });
    expect(await journalOf(replica, 'd-1')).toEqual(['saga-started', 'delivery-dead:a']);
    expect(
      blockedEvents.map(({ sagaId, step, phase, attempt, messageId, eventType }) => ({
        sagaId,
        step,
        phase,
        attempt,
        messageId,
        eventType,
      })),
    ).toEqual([
      {
        sagaId: 'd-1',
        step: 'a',
        phase: 'execute',
        attempt: 1,
        messageId: commandId,
        eventType: 'saga.command',
      },
    ]);
    // Вставший — в отчёте ждущих человека и в срезе.
    expect((await replica.admin.stuckReport()).map((item) => `${item.id}:${item.status}`)).toEqual([
      'd-1:blocked',
    ]);
    expect((await replica.admin.stats(START, clock.now)).map((s) => s.blocked)).toEqual([1]);
    // Второй проход: уже blocked — ни перевода, ни хука.
    expect((await watchOf(replica).runOnce()).blocked).toEqual([]);
    expect(blockedEvents).toHaveLength(1);

    broken.clear();
    await replica.admin.retry('d-1', 'шаг починен');
    const retried = await instanceOf(replica, 'd-1');
    expect({ status: retried.status, generation: retried.generation, attempt: retried.attempt }).toEqual({
      status: SAGA_STATUS.running,
      generation: 1,
      attempt: 1,
    });
    // DEAD прошлого поколения остался в outbox — живой экземпляр он не останавливает.
    clock.now = new Date(clock.now.getTime() + MINUTE);
    expect((await watchOf(replica).runOnce()).blocked).toEqual([]);
    expect(await outboxStatusOf(commandId)).toBe('dead');
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'd-1')).status).toBe(SAGA_STATUS.completed);
  }, 30_000);

  it('DEAD прошлой ПОПЫТКИ того же шага не блокирует экземпляр, ждущий следующую', async () => {
    let calls = 0;
    const flaky = defineSaga<{ n: number }>('flaky', { version: 1 })
      .step('a', {
        execute: async () => {
          calls += 1;
          if (calls === 1) {
            throw new Error('первая попытка');
          }
          return {};
        },
        retry: { maxAttempts: 3, backoffMs: 10 * MINUTE, maxBackoffMs: 10 * MINUTE },
      })
      .build();
    const replica = createReplica({ pool: database.newPool(4), clock, definitions: [flaky] });
    await replica.start(flaky, 'f-1', { n: 1 });
    await replica.relay.runOnce();
    const instance = await instanceOf(replica, 'f-1');
    expect({ attempt: instance.attempt, waits: instance.wakeAt !== null }).toEqual({
      attempt: 2,
      waits: true,
    });
    const first = sagaMessageId.command({
      sagaId: 'f-1',
      incarnation: instance.incarnation,
      generation: 0,
      phase: 'execute',
      stepIndex: 0,
      attempt: 1,
    });
    // Команда первой попытки — в DEAD (как если бы её доставка исчерпала попытки).
    await database.pool.query(`UPDATE ${TEST_TABLES.outbox} SET status = 'dead' WHERE id = $1`, [first]);
    expect(await outboxStatusOf(first)).toBe('dead');
    // Пауза повтора прошла, а доставки второй попытки ещё не было: экземпляр — кандидат сторожа.
    clock.now = new Date(clock.now.getTime() + 11 * MINUTE);
    const report = await watchOf(replica).runOnce();
    expect({
      checked: report.checked,
      blocked: report.blocked,
      status: (await instanceOf(replica, 'f-1')).status,
    }).toEqual({
      checked: 1,
      blocked: [],
      status: SAGA_STATUS.running,
    });
  });

  it('компенсация отказывает на каждой доставке: blocked в фазе компенсации, повтор — снова компенсация', async () => {
    const replica = replicaFor();
    broken.add('b:reject');
    broken.add('a:compensate');
    await replica.start(twoSteps, 'c-1', { n: 1 });
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'c-1')).status).toBe(SAGA_STATUS.compensating);

    expect((await watchOf(replica).runOnce()).blocked).toEqual(['c-1']);
    expect(blockedEvents.map((event) => `${event.step}:${event.phase}`)).toEqual(['a:compensate']);
    broken.clear();
    await replica.admin.retry('c-1', 'компенсация починена');
    expect((await instanceOf(replica, 'c-1')).status).toBe(SAGA_STATUS.compensating);
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'c-1')).status).toBe(SAGA_STATUS.compensated);
  }, 30_000);

  it('таймер ответа удалённого шага в DEAD — blocked по таймеру', async () => {
    const remote = defineSaga<{ n: number }>('remote-dead', { version: 1 })
      .remoteStep('charge', {
        command: () => ({ eventType: 'billing.charge', payload: {} }),
        timeoutMs: MINUTE,
      })
      .build();
    const replica = createReplica({ pool: database.newPool(4), clock, definitions: [remote] });
    await replica.start(remote, 'r-1', { n: 1 });
    await replica.relay.runOnce();
    const instance = await instanceOf(replica, 'r-1');
    const timer = sagaMessageId.timer({
      sagaId: 'r-1',
      incarnation: instance.incarnation,
      generation: 0,
      phase: 'execute',
      stepIndex: 0,
      attempt: 1,
    });
    await database.pool.query(`UPDATE ${TEST_TABLES.outbox} SET status = 'dead' WHERE id = $1`, [timer]);
    // До срока таймера экземпляр ждёт штатно и не проверяется.
    expect((await watchOf(replica).runOnce()).checked).toBe(0);
    clock.now = new Date(clock.now.getTime() + 2 * MINUTE);
    expect((await watchOf(replica).runOnce()).blocked).toEqual(['r-1']);
    expect(blockedEvents.map((event) => `${event.messageId}:${event.eventType}`)).toEqual([
      `${timer}:saga.timer`,
    ]);
  });

  it('два сторожа над одним экземпляром: второй под блокировкой видит уже blocked — перевод и хук один раз', async () => {
    const replica = replicaFor();
    broken.add('a');
    await replica.start(twoSteps, 'two-1', { n: 1 });
    await pump(clock, [replica]);
    // Первый сторож прочёл кандидатов и стоит перед своей транзакцией, пока второй не закончит.
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered: () => void = () => undefined;
    const waiting = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const late = new SagaStallWatch({
      db: replica.db,
      transaction: async (work) => {
        entered();
        await gate;
        return await replica.transaction(work);
      },
      outbox: replica.outbox,
      tables: TABLES,
      stalledAfterMs: 30 * MINUTE,
      clock: () => clock.now,
      metrics: { blocked: (event) => blockedEvents.push(event) },
    });
    const first = late.runOnce();
    await waiting;
    const second = await watchOf(replica).runOnce();
    release();
    expect({ first: (await first).blocked, second: second.blocked, hooks: blockedEvents.length }).toEqual({
      first: [],
      second: ['two-1'],
      hooks: 1,
    });
    expect(
      (await journalOf(replica, 'two-1')).filter((entry) => entry.startsWith('delivery-dead')),
    ).toHaveLength(1);
  }, 30_000);

  it('неподвижный без DEAD — stalled один раз на эпизод, без повтора; новый процесс говорит снова; сдвинувшийся забыт', async () => {
    const replica = replicaFor();
    await replica.start(twoSteps, 's-1', { n: 1 });
    const watch = watchOf(replica);
    expect((await watch.runOnce()).stalled).toEqual([]);
    clock.now = new Date(clock.now.getTime() + 31 * MINUTE);
    expect((await watch.runOnce()).stalled).toEqual(['s-1']);
    expect((await watch.runOnce()).stalled).toEqual([]);
    expect(stalledEvents).toEqual(['s-1:a:running']);
    // Повтора нет: команда как была, экземпляр не сдвинут.
    expect((await instanceOf(replica, 's-1')).generation).toBe(0);
    expect((await watchOf(replica).runOnce()).stalled).toEqual(['s-1']);
    await pump(clock, [replica]);
    expect((await watch.runOnce()).stalled).toEqual([]);
  });

  it('неподвижных больше batchSize: за круг проходов о каждом сказано ровно один раз', async () => {
    const replica = replicaFor();
    const ids = ['s-1', 's-2', 's-3', 's-4', 's-5'];
    for (const id of ids) {
      await replica.start(twoSteps, id, { n: 1 });
    }
    clock.now = new Date(clock.now.getTime() + 31 * MINUTE);
    const watch = new SagaStallWatch({
      db: replica.db,
      transaction: replica.transaction,
      outbox: replica.outbox,
      tables: TABLES,
      stalledAfterMs: 30 * MINUTE,
      batchSize: 2,
      clock: () => clock.now,
      metrics: { stalled: (event) => stalledEvents.push(event.sagaId) },
    });
    const told: string[] = [];
    for (let pass = 0; pass < 6; pass += 1) {
      told.push(...(await watch.runOnce()).stalled);
    }
    expect([...told].sort()).toEqual(ids);
    expect([...stalledEvents].sort()).toEqual(ids);
  });

  it('хук stalled отказал — эпизод не считается сказанным: следующий проход повторяет, затем молчит', async () => {
    const replica = replicaFor();
    await replica.start(twoSteps, 's-1', { n: 1 });
    clock.now = new Date(clock.now.getTime() + 31 * MINUTE);
    let fail = true;
    const calls: string[] = [];
    const watch = new SagaStallWatch({
      db: replica.db,
      transaction: replica.transaction,
      outbox: replica.outbox,
      tables: TABLES,
      stalledAfterMs: 30 * MINUTE,
      clock: () => clock.now,
      metrics: {
        stalled: (event) => {
          calls.push(event.sagaId);
          if (fail) {
            fail = false;
            throw new Error('канал метрик отказал');
          }
        },
      },
    });
    await watch.runOnce();
    await watch.runOnce();
    await watch.runOnce();
    expect(calls).toEqual(['s-1', 's-1']);
  });

  it('настройка: порог stalled обязателен, batchSize в пределах, outbox — хранилище', () => {
    const base = {
      db: {},
      transaction: async () => undefined,
      tables: TABLES,
      outbox: { deadLetters: async () => [] },
    };
    expect(() => new SagaStallWatch({ ...base, stalledAfterMs: 0 } as never)).toThrow(SagaConfigError);
    expect(() => new SagaStallWatch({ ...base, stalledAfterMs: MINUTE, batchSize: 0 } as never)).toThrow(
      SagaConfigError,
    );
    expect(() => new SagaStallWatch({ ...base, outbox: {}, stalledAfterMs: MINUTE } as never)).toThrow(
      /deadLetters/,
    );
    expect(() => new SagaStallWatch({ ...base, stalledAfterMs: MINUTE } as never)).not.toThrow();
  });
});
