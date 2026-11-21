import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaStepRejectedError } from '../errors/saga-step-rejected.error';
import { sagaReplyOf } from '../remote/saga-reply-of';
import { paymentSaga } from '../testing/billing-participant';
import {
  createReplica,
  instanceOf,
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
  waitForLockWaiters,
} from '../testing/saga-test-database';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';

const HELD_MS = 15_000;

/**
 * Время перехода снимается ПОСЛЕ ожидания строки экземпляра: пока его держит
 * чужая транзакция (шаг, идущий секундами), часы уходят вперёд, и снятое до
 * ожидания время увело бы `updated_at` и журнал назад. Каждое место, где
 * переход ждёт строку, — отдельной проверкой.
 */
describeDb('время перехода — после блокировки строки', () => {
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

  /**
   * Строку держит чужая транзакция; операция встаёт в ожидание; часы уходят на
   * 15 с; строка отпущена. Возвращает время новых записей журнала и updated_at.
   */
  const whileRowHeld = async (
    replica: SagaReplica,
    id: string,
    operation: () => Promise<unknown>,
  ): Promise<{ updatedAtMs: number; journalAtMs: number[] }> => {
    const before = (await replica.admin.show(id))?.journal.length ?? 0;
    const holder = await database.pool.connect();
    try {
      await holder.query('BEGIN');
      await holder.query(`SELECT 1 FROM ${TEST_TABLES.instances} WHERE id = $1 FOR UPDATE`, [id]);
      const pending = operation();
      await waitForLockWaiters(database.pool, 'FOR UPDATE', 1);
      clock.now = new Date(clock.now.getTime() + HELD_MS);
      await holder.query('COMMIT');
      await pending;
    } finally {
      holder.release();
    }
    const shown = await replica.admin.show(id);
    return {
      updatedAtMs: (shown?.instance.updatedAt.getTime() ?? 0) - START.getTime(),
      journalAtMs: (shown?.journal ?? []).slice(before).map((entry) => entry.at.getTime() - START.getTime()),
    };
  };

  const pendingPayload = async (eventType: string): Promise<Record<string, unknown>> => {
    const { rows } = await database.pool.query<{ payload: Record<string, unknown> }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = $1 AND status = 'pending'
        ORDER BY created_at DESC LIMIT 1`,
      [eventType],
    );
    if (rows[0] === undefined) {
      throw new Error(`нет ждущего ${eventType}`);
    }
    return rows[0].payload;
  };

  /** Сага с удалённым шагом: reserve выполнен, команда charge ждёт исполнителя. */
  const atCharge = async (): Promise<SagaReplica> => {
    const definition = paymentSaga();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'held', { orderId: '1' });
    await replica.relay.runOnce();
    expect((await instanceOf(replica, 'held')).stepIndex).toBe(1);
    return replica;
  };

  /** Та же сага, команда charge отправлена (участник не подписан — ответа нет). */
  const dispatched = async (): Promise<SagaReplica> => {
    const replica = await atCharge();
    await replica.orchestrator.handle('saga.command', await pendingPayload('saga.command'));
    return replica;
  };

  const expectAfterHold = (result: { updatedAtMs: number; journalAtMs: number[] }, entries: number): void => {
    expect(result).toEqual({ updatedAtMs: HELD_MS, journalAtMs: Array(entries).fill(HELD_MS) });
  };

  it('cancel', async () => {
    const replica = await atCharge();
    expectAfterHold(
      await whileRowHeld(replica, 'held', () =>
        replica.transaction((tx) => replica.orchestrator.cancel(tx, 'held', 'отмена')),
      ),
      1,
    );
  });

  it('отправка команды удалённого шага (и срок таймера — от неё)', async () => {
    const replica = await atCharge();
    const payload = await pendingPayload('saga.command');
    expectAfterHold(
      await whileRowHeld(replica, 'held', () => replica.orchestrator.handle('saga.command', payload)),
      1,
    );
    expect((await instanceOf(replica, 'held')).wakeAt?.getTime()).toBe(START.getTime() + HELD_MS + 30_000);
  });

  it('ответ участника', async () => {
    const replica = await dispatched();
    const { sagaCommand } = (await pendingPayload('billing.charge')) as { sagaCommand: SagaCommandMeta };
    expectAfterHold(
      await whileRowHeld(replica, 'held', () =>
        replica.orchestrator.handleReply(sagaReplyOf(sagaCommand, { ok: true, data: { chargeId: 'c' } })),
      ),
      1,
    );
  });

  it('таймер ответа', async () => {
    const replica = await dispatched();
    const timer = await pendingPayload('saga.timer');
    expectAfterHold(
      await whileRowHeld(replica, 'held', () => replica.orchestrator.handle('saga.timer', timer)),
      1,
    );
  });

  describe('ручной разбор застрявшего', () => {
    const stuckSaga = async (): Promise<SagaReplica> => {
      const definition = defineSaga<{ orderId: string }>('stuck-time', { version: 1 })
        .step('a', {
          execute: async () => ({ a: 1 }),
          compensate: async () => {
            throw new Error('компенсация лежит');
          },
          compensationRetry: { maxAttempts: 1 },
        })
        .step('b', {
          execute: async () => {
            throw new SagaStepRejectedError('нет');
          },
        })
        .build();
      const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
      await replica.start(definition, 'held', { orderId: '1' });
      await pump(clock, [replica]);
      expect((await instanceOf(replica, 'held')).status).toBe(SAGA_STATUS.stuck);
      clock.now = new Date(START);
      // Строка двигалась раньше START: «назад» видно и без зажима по updated_at.
      await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET updated_at = $1`, [
        new Date(START.getTime() - 60_000),
      ]);
      return replica;
    };

    it('retry', async () => {
      const replica = await stuckSaga();
      expectAfterHold(
        await whileRowHeld(replica, 'held', () => replica.admin.retry('held', 'склад поднят')),
        1,
      );
    });

    it('resolve', async () => {
      const replica = await stuckSaga();
      expectAfterHold(
        await whileRowHeld(replica, 'held', () => replica.admin.resolve('held', 'возврат вручную')),
        1,
      );
    });
  });

  /** Строка «сдвинута» репликой, чьи часы впереди на `aheadMs`: updated_at в будущем. */
  const movedAhead = async (id: string, aheadMs: number): Promise<void> => {
    await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET updated_at = $1 WHERE id = $2`, [
      new Date(START.getTime() + aheadMs),
      id,
    ]);
  };

  /** Новые события outbox после операции: тип и срок/создание относительно START. */
  const newMessages = async (
    operation: () => Promise<unknown>,
  ): Promise<{ type: string; dueMs: number; createdMs: number }[]> => {
    const before = new Set(
      (await database.pool.query<{ id: string }>(`SELECT id FROM ${TEST_TABLES.outbox}`)).rows.map(
        (r) => r.id,
      ),
    );
    await operation();
    const { rows } = await database.pool.query<{ id: string; type: string; due: Date; created: Date }>(
      `SELECT id, event_type AS type, next_attempt_at AS due, created_at AS created
         FROM ${TEST_TABLES.outbox} ORDER BY event_type`,
    );
    return rows
      .filter((row) => !before.has(row.id))
      .map((row) => ({
        type: row.type,
        dueMs: row.due.getTime() - START.getTime(),
        createdMs: row.created.getTime() - START.getTime(),
      }));
  };

  const stuckAt = async (): Promise<SagaReplica> => {
    const definition = defineSaga<{ orderId: string }>('stuck-skew', { version: 1 })
      .step('a', {
        execute: async () => ({ a: 1 }),
        compensate: async () => {
          throw new Error('компенсация лежит');
        },
        compensationRetry: { maxAttempts: 1 },
      })
      .step('b', {
        execute: async () => {
          throw new SagaStepRejectedError('нет');
        },
      })
      .build();
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'held', { orderId: '1' });
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'held')).status).toBe(SAGA_STATUS.stuck);
    clock.now = new Date(START);
    return replica;
  };

  const AHEAD = 5 * 60_000;

  /**
   * Часы другой реплики (или админки на ноутбуке) впереди на 5 минут: она
   * сдвинула экземпляр, updated_at — в будущем. Переход на реплике с верными
   * часами: ОТМЕТКА не уходит назад (updated_at, журнал — не раньше сдвига), а
   * СРОКИ команд — по своим часам. Зажим в сроках держал бы следующую команду
   * до тех пор, пока реальное время не догонит чужие часы.
   */
  it.each([
    [
      'cancel: команда компенсации — сразу',
      async () => {
        const replica = await atCharge();
        await movedAhead('held', AHEAD);
        return {
          replica,
          run: () => replica.transaction((tx) => replica.orchestrator.cancel(tx, 'held', 'отмена')),
        };
      },
      [{ type: 'saga.command', dueMs: 0, createdMs: 0 }],
    ],
    [
      'отправка команды участнику: команда — сразу, таймер — через timeoutMs от своих часов',
      async () => {
        const replica = await atCharge();
        const payload = await pendingPayload('saga.command');
        await movedAhead('held', AHEAD);
        return { replica, run: () => replica.orchestrator.handle('saga.command', payload) };
      },
      [
        { type: 'billing.charge', dueMs: 0, createdMs: 0 },
        { type: 'saga.timer', dueMs: 30_000, createdMs: 0 },
      ],
    ],
    [
      'ответ участника: следующий шаг — сразу',
      async () => {
        const replica = await dispatched();
        const { sagaCommand } = (await pendingPayload('billing.charge')) as { sagaCommand: SagaCommandMeta };
        await movedAhead('held', AHEAD);
        return {
          replica,
          run: () =>
            replica.orchestrator.handleReply(sagaReplyOf(sagaCommand, { ok: true, data: { chargeId: 'c' } })),
        };
      },
      [{ type: 'saga.command', dueMs: 0, createdMs: 0 }],
    ],
    [
      'таймер ответа: повтор — через паузу (750 мс) от своих часов',
      async () => {
        const replica = await dispatched();
        const timer = await pendingPayload('saga.timer');
        await movedAhead('held', AHEAD);
        return { replica, run: () => replica.orchestrator.handle('saga.timer', timer) };
      },
      [{ type: 'saga.command', dueMs: 750, createdMs: 0 }],
    ],
    [
      'ручной повтор застрявшего: команда — сразу',
      async () => {
        const replica = await stuckAt();
        await movedAhead('held', AHEAD);
        return { replica, run: () => replica.admin.retry('held', 'склад поднят') };
      },
      [{ type: 'saga.command', dueMs: 0, createdMs: 0 }],
    ],
  ] as const)('часы другой реплики впереди — %s; отметка не назад', async (_name, prepare, expected) => {
    const { replica, run } = await prepare();
    const before = (await replica.admin.show('held'))?.journal.length ?? 0;
    expect(await newMessages(run)).toEqual(expected);
    const shown = await replica.admin.show('held');
    expect({
      updatedAtMs: (shown?.instance.updatedAt.getTime() ?? 0) - START.getTime(),
      journalAtMs: [
        ...new Set((shown?.journal ?? []).slice(before).map((e) => e.at.getTime() - START.getTime())),
      ],
    }).toEqual({ updatedAtMs: AHEAD, journalAtMs: [AHEAD] });
  });

  it('часы другой реплики впереди: сага с удалённым шагом доезжает за секунды своего времени, а не через 5 минут', async () => {
    const fast: TestClock = { now: new Date(START.getTime() + AHEAD) };
    const definition = defineSaga<{ o: string }>('skew', { version: 1 })
      .remoteStep('charge', {
        command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
        timeoutMs: 60 * 60_000,
      })
      .step('ship', { execute: async () => ({ shipped: true }) })
      .build();
    const good = createReplica({ pool: database.pool, clock, definitions: [definition] });
    const skewed = createReplica({ pool: database.pool, clock: fast, definitions: [definition] });
    good.handlers.register('billing.charge', async () => undefined);
    await good.start(definition, 's-1', { o: '1' });
    // Команду участнику отправляет реплика с часами впереди.
    await skewed.relay.runOnce();
    const { sagaCommand } = (await pendingPayload('billing.charge')) as { sagaCommand: SagaCommandMeta };
    clock.now = new Date(clock.now.getTime() + 1_000);
    await good.orchestrator.handleReply(sagaReplyOf(sagaCommand, { ok: true }));
    for (let second = 0; second < 5; second += 1) {
      await good.relay.runOnce();
      clock.now = new Date(clock.now.getTime() + 1_000);
    }
    expect((await instanceOf(good, 's-1')).status).toBe(SAGA_STATUS.completed);
  });

  it.each([
    [
      'cancel',
      async (replica: SagaReplica) =>
        replica.transaction((tx) => replica.orchestrator.cancel(tx, 'held', 'отмена')),
    ],
    ['resolve', async (replica: SagaReplica) => replica.admin.resolve('held', 'возврат вручную')],
    ['retry', async (replica: SagaReplica) => replica.admin.retry('held', 'склад поднят')],
  ] as const)(
    'часы реплики отстают от последнего сдвига экземпляра — %s: время перехода не раньше него',
    async (name, operation) => {
      const replica = name === 'cancel' ? await atCharge() : await stuckAt();
      await movedAhead('held', 60_000);
      await operation(replica);
      const shown = await replica.admin.show('held');
      expect({
        updatedAtMs: (shown?.instance.updatedAt.getTime() ?? 0) - START.getTime(),
        lastAtMs: (shown?.journal.at(-1)?.at.getTime() ?? 0) - START.getTime(),
      }).toEqual({ updatedAtMs: 60_000, lastAtMs: 60_000 });
    },
  );
});
