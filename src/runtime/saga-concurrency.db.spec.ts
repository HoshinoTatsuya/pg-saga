import type { Client } from 'pg';
import { Client as PgClient } from 'pg';

import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { sagaReplyOf } from '../remote/saga-reply-of';
import {
  type BillingScript,
  billingParticipant,
  newBillingScript,
  paymentSaga,
} from '../testing/billing-participant';
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
  waitForLockWaiters,
} from '../testing/saga-test-database';
import { type FailureScript, newScript, orderSaga } from '../testing/test-sagas';

/**
 * Несколько исполнителей одной базы: каждое сообщение применяется один раз,
 * один экземпляр в каждый момент двигает один исполнитель. Сравнение —
 * множествами: порядок, в котором реплики забрали события, не определён.
 */
describeDb('SagaOrchestrator: несколько реплик', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let script: FailureScript;
  let holder: Client | null;

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
    script = newScript();
    holder = null;
  });

  afterEach(async () => {
    script.inside = undefined;
    if (holder !== null) {
      await holder.query('ROLLBACK').catch(() => undefined);
      await holder.end().catch(() => undefined);
    }
  });

  /** Две реплики со своими пулами и общим определением. */
  const twoReplicas = (scripted: FailureScript) => {
    const definition = orderSaga(scripted);
    const pair = [database.newPool(6), database.newPool(6)].map((pool) =>
      createReplica({ pool, clock, definitions: [definition] }),
    ) as [SagaReplica, SagaReplica];
    return { definition, pair };
  };

  it('две реплики гоняют 16 саг одновременно: каждый шаг выполнен ровно один раз, все завершены', async () => {
    script.execute.set('charge', 1);
    const { definition, pair } = twoReplicas(script);
    const ids = Array.from({ length: 16 }, (_, index) => `m-${String(index).padStart(2, '0')}`);
    for (const [index, id] of ids.entries()) {
      await pair[index % 2 === 0 ? 0 : 1].start(definition, id, { orderId: id });
    }

    await pump(clock, pair);

    const { rows: effects } = await database.pool.query<{ key: string }>(
      "SELECT saga_id || ':' || step AS key FROM effects WHERE phase = 'execute'",
    );
    const expected = ids.flatMap((id) =>
      ['reserve', 'notify', 'charge', 'ship'].map((step) => `${id}:${step}`),
    );
    expect(new Set(effects.map((row) => row.key))).toEqual(new Set(expected));
    expect(effects).toHaveLength(expected.length);
    const { rows: statuses } = await database.pool.query<{ status: string; n: number }>(
      `SELECT status, count(*)::int AS n FROM ${TEST_TABLES.instances} GROUP BY status`,
    );
    expect(statuses).toEqual([{ status: SAGA_STATUS.completed, n: 16 }]);
    const { rows: completions } = await database.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${TEST_TABLES.journal} WHERE event = 'step-completed'`,
    );
    expect(completions[0]?.n).toBe(16 * 4);
    // Выборка не выродилась: работали обе реплики.
    const { rows: claimers } = await database.pool.query<{ n: number }>(
      `SELECT count(DISTINCT claimed_by)::int AS n FROM ${TEST_TABLES.outbox}`,
    );
    expect(claimers[0]?.n).toBeGreaterThan(1);
  });

  it('одна команда доставлена двум репликам одновременно: шаг выполнен один раз, второй — duplicate', async () => {
    const { definition, pair } = twoReplicas(script);
    const [first, second] = pair;
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    script.inside = async (step) => {
      if (step === 'reserve') {
        await gate;
      }
    };
    await first.start(definition, 'd-1', { orderId: 'd' });
    const { rows } = await database.pool.query<{ payload: unknown }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.command'`,
    );
    const payload = rows[0]?.payload;

    const racing = [
      first.orchestrator.handle('saga.command', payload),
      second.orchestrator.handle('saga.command', payload),
    ];
    // Гонка выстроена: один исполнитель внутри шага, второй ждёт его блокировку.
    await waitForLockWaiters(database.pool, '', 1);
    release();
    const results = await Promise.all(racing);

    expect(results.sort()).toEqual([SAGA_HANDLED.applied, SAGA_HANDLED.duplicate].sort());
    const { rows: effects } = await database.pool.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM effects WHERE saga_id = 'd-1' AND step = 'reserve'",
    );
    expect(effects[0]?.n).toBe(1);
    expect(
      (await journalOf(first, 'd-1')).filter((entry) => entry === 'step-completed:reserve'),
    ).toHaveLength(1);
  });

  describe('разные сообщения одного экземпляра одновременно', () => {
    let billing: BillingScript;
    const payment = paymentSaga();

    /**
     * Ответ «успех» и таймер ОДНОЙ попытки приходят на две реплики сразу.
     * Ответ идёт первым и держит строку экземпляра; таймер обязан дождаться
     * его фиксации и увидеть сдвинутое состояние (stale). Без блокировки
     * строки таймер прочитал бы старое состояние и записал повтор поверх
     * успеха — потерянное обновление. Внешняя блокировка журнала держит обе
     * транзакции до того, как первая зафиксируется.
     */
    it('успех и таймер одной попытки: применяется только первый, второй — stale', async () => {
      billing = newBillingScript();
      billing.execute.push('silent');
      const pools = [database.newPool(6), database.newPool(6)];
      const [first, second] = pools.map((pool) =>
        createReplica({ pool, clock, definitions: [payment], participants: billingParticipant(billing) }),
      ) as [SagaReplica, SagaReplica];
      await first.start(payment, 'l-1', { orderId: 'l' });
      for (let pass = 0; pass < 3; pass += 1) {
        await first.relay.runOnce();
      }
      const meta = billing.received[0];
      if (meta === undefined) {
        throw new Error('участник не получил команду');
      }
      const { rows } = await database.pool.query<{ payload: unknown }>(
        `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'saga.timer'`,
      );
      holder = new PgClient({ connectionString: database.url });
      await holder.connect();
      await holder.query('BEGIN');
      await holder.query(`LOCK TABLE ${TEST_TABLES.journal} IN SHARE ROW EXCLUSIVE MODE`);

      const reply = first.orchestrator.handleReply(
        sagaReplyOf(meta, { ok: true, data: { chargeId: 'ch-l' } }),
      );
      await waitForLockWaiters(database.pool, `INSERT INTO "${TEST_TABLES.journal}"`, 1);
      const timer = second.orchestrator.handle('saga.timer', rows[0]?.payload);
      await waitForLockWaiters(database.pool, '', 2);
      await holder.query('ROLLBACK');

      expect({ reply: await reply, timer: await timer }).toEqual({
        reply: SAGA_HANDLED.applied,
        timer: SAGA_HANDLED.stale,
      });
      const instance = await instanceOf(first, 'l-1');
      expect({
        step: instance.steps[instance.stepIndex],
        attempt: instance.attempt,
        status: instance.status,
      }).toEqual({
        step: 'ship',
        attempt: 1,
        status: SAGA_STATUS.running,
      });
      const { rows: retries } = await database.pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox}
          WHERE event_type = 'saga.command' AND payload->>'stepIndex' = '1' AND payload->>'attempt' = '2'`,
      );
      expect(retries[0]?.n).toBe(0);
      expect(await journalOf(first, 'l-1')).not.toContain('step-timed-out:charge');
    });
  });
});
