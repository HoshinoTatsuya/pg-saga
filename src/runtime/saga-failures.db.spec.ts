import { CommitOutcomeUnknownError } from '@hoshinotatsuya/pg-runtime';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { Client } from 'pg';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import {
  createReplica,
  effectsOf,
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
import { type FailureScript, newScript, orderSaga } from '../testing/test-sagas';
import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';
import { sagaTransactionRunner } from './saga-transaction-runner';

const ALL_EXECUTED = ['reserve:execute', 'notify:execute', 'charge:execute', 'ship:execute'];

/** Обрыв посреди шага и неизвестный исход COMMIT: сага продолжается, шаг не задваивается. */
describeDb('SagaOrchestrator: отказы инфраструктуры', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let script: FailureScript;
  let release: () => void;

  /** Ворота внутри шага charge первой попытки: шаг стоит, пока проверка не отпустит. */
  const gateCharge = (): Promise<number> =>
    new Promise((entered) => {
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      script.inside = async (step, args) => {
        if (step !== 'charge' || args.attempt !== 1 || script.inside === undefined) {
          return;
        }
        script.inside = undefined;
        const { rows } = await args.tx.execute(sql`SELECT pg_backend_pid() AS pid`);
        entered(Number((rows[0] as { pid: number }).pid));
        await gate;
      };
    });

  const terminate = async (pid: number): Promise<void> => {
    const admin = new Client({ connectionString: database.adminUrl });
    await admin.connect();
    try {
      const { rows } = await admin.query<{ done: boolean }>('SELECT pg_terminate_backend($1) AS done', [pid]);
      expect(rows[0]?.done).toBe(true);
    } finally {
      await admin.end();
    }
  };

  const commandRow = async (stepIndex: number) =>
    (
      await database.pool.query<{ status: string; attempts: number }>(
        `SELECT status, attempts FROM ${TEST_TABLES.outbox}
          WHERE event_type = 'saga.command' AND payload->>'stepIndex' = $1 AND payload->>'attempt' = '1'`,
        [String(stepIndex)],
      )
    ).rows;

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
    release = () => undefined;
  });

  afterEach(() => {
    release();
    script.inside = undefined;
  });

  it('соединение шага оборвано сервером посреди шага: эффект откатился, повтор доставки доводит сагу без второго шага', async () => {
    const definition = orderSaga(script);
    const replica = createReplica({ pool: database.newPool(4), clock, definitions: [definition] });
    const entered = gateCharge();
    await replica.start(definition, 'k-1', { orderId: 'k' });

    const running = pump(clock, [replica]);
    await terminate(await entered);
    release();
    await running;

    const instance = await instanceOf(replica, 'k-1');
    expect({ status: instance.status, failure: instance.failure }).toEqual({
      status: SAGA_STATUS.completed,
      failure: null,
    });
    expect(await effectsOf(database.pool, 'k-1')).toEqual(ALL_EXECUTED);
    // Обрыв — не отказ шага: попытка шага не потрачена, повторила доставка outbox
    // (attempts outbox — число неудачных доставок).
    expect((await journalOf(replica, 'k-1')).filter((entry) => entry.endsWith(':charge'))).toEqual([
      'step-completed:charge',
    ]);
    expect(await commandRow(2)).toEqual([{ status: 'published', attempts: 1 }]);
  });

  it('процесс умер посреди шага (код встал, соединение оборвано): новый процесс после окна перехвата доводит сагу', async () => {
    const definition = orderSaga(script);
    const dead = createReplica({ pool: database.newPool(4), clock, definitions: [definition] });
    const entered = gateCharge();
    await dead.start(definition, 'k-2', { orderId: 'k' });
    await dead.relay.runOnce();
    await dead.relay.runOnce();
    // Третий проход доходит до шага charge и навсегда встаёт внутри него: процесс «умер».
    const abandoned = dead.relay.runOnce();
    await terminate(await entered);
    expect(await commandRow(2)).toEqual([{ status: 'dispatching', attempts: 0 }]);

    // Новый процесс: свой пул, своё определение (новый объект с теми же шагами и версией).
    const restarted = createReplica({ pool: database.newPool(4), clock, definitions: [orderSaga(script)] });
    await pump(clock, [restarted]);

    expect((await instanceOf(restarted, 'k-2')).status).toBe(SAGA_STATUS.completed);
    expect(await effectsOf(database.pool, 'k-2')).toEqual(ALL_EXECUTED);
    expect(
      (await journalOf(restarted, 'k-2')).filter((entry) => entry === 'step-completed:charge'),
    ).toHaveLength(1);
    // «Умерший» код просыпается: его соединения нет, его отметки не находят свой захват.
    release();
    await abandoned;
    expect(await effectsOf(database.pool, 'k-2')).toEqual(ALL_EXECUTED);
  });

  describe('CommitOutcomeUnknownError на транзакции шага', () => {
    /**
     * Обёртка транзакции: на транзакции, где выполнялся шаг charge, —
     * `committed`: COMMIT прошёл, вызывающему — «исход неизвестен»;
     * `rolled-back`: транзакция откатилась, вызывающему — тот же отказ.
     * Обе реальности возможны при обрыве после отправки COMMIT.
     */
    const unknownOutcome =
      (mode: 'committed' | 'rolled-back', state: { armed: boolean; sawCharge: boolean }) =>
      (inner: SagaTransactionRunner): SagaTransactionRunner =>
      async <T>(work: Parameters<SagaTransactionRunner>[0]): Promise<T> => {
        const cut = new Error('обрыв после отправки COMMIT');
        state.sawCharge = false;
        if (mode === 'committed') {
          const result = (await inner(work)) as T;
          if (state.armed && state.sawCharge) {
            state.armed = false;
            throw new CommitOutcomeUnknownError(cut);
          }
          return result;
        }
        try {
          return (await inner(async (tx) => {
            const result = await work(tx);
            if (state.armed && state.sawCharge) {
              state.armed = false;
              throw cut;
            }
            return result;
          })) as T;
        } catch (error) {
          throw error === cut ? new CommitOutcomeUnknownError(cut) : error;
        }
      };

    it.each(['committed', 'rolled-back'] as const)(
      'исход %s: сага доходит до конца, шаг charge выполнен ровно один раз',
      async (mode) => {
        const state = { armed: true, sawCharge: false };
        script.inside = async (step) => {
          if (step === 'charge') {
            state.sawCharge = true;
          }
        };
        const definition = orderSaga(script);
        const replica = createReplica({
          pool: database.pool,
          clock,
          definitions: [definition],
          transaction: unknownOutcome(mode, state),
        });
        await replica.start(definition, `u-${mode}`, { orderId: 'u' });

        await pump(clock, [replica]);

        expect(state.armed).toBe(false);
        expect((await instanceOf(replica, `u-${mode}`)).status).toBe(SAGA_STATUS.completed);
        expect(await effectsOf(database.pool, `u-${mode}`)).toEqual(ALL_EXECUTED);
        expect((await journalOf(replica, `u-${mode}`)).filter((entry) => entry.endsWith(':charge'))).toEqual([
          'step-completed:charge',
        ]);
        expect(await commandRow(2)).toEqual([{ status: 'published', attempts: 1 }]);
      },
    );

    /**
     * Атомарность: транзакция шага откатилась ПОСЛЕ того, как шаг и переход
     * записаны, — не остаётся ни эффекта, ни записи о шаге, ни сдвига
     * экземпляра, ни следующей команды.
     */
    it('откат транзакции шага снимает всё вместе: эффект, журнал, состояние, следующую команду', async () => {
      const state = { armed: true, sawCharge: false };
      script.inside = async (step) => {
        if (step === 'charge') {
          state.sawCharge = true;
        }
      };
      const definition = orderSaga(script);
      const replica: SagaReplica = createReplica({
        pool: database.pool,
        clock,
        definitions: [definition],
        transaction: unknownOutcome('rolled-back', state),
      });
      await replica.start(definition, 'a-1', { orderId: 'a' });
      for (let pass = 0; pass < 3; pass += 1) {
        await replica.relay.runOnce();
      }

      expect(state.armed).toBe(false);
      const instance = await instanceOf(replica, 'a-1');
      expect({
        step: instance.steps[instance.stepIndex],
        attempt: instance.attempt,
        context: instance.context,
      }).toEqual({ step: 'charge', attempt: 1, context: { orderId: 'a', reservationId: 'r-a' } });
      expect(await effectsOf(database.pool, 'a-1')).toEqual(['reserve:execute', 'notify:execute']);
      expect(await journalOf(replica, 'a-1')).toEqual([
        'saga-started',
        'step-completed:reserve',
        'step-completed:notify',
      ]);
      const { rows } = await database.pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox}
          WHERE event_type = 'saga.command' AND payload->>'stepIndex' = '3'`,
      );
      expect(rows[0]?.n).toBe(0);
    });
  });

  /**
   * Последний запрос шага начат поздно: без потолка-остатка он шёл бы на
   * сервере весь свой statement_timeout (потолок шага целиком), ROLLBACK
   * стоял бы за ним, а запись отказа не дождалась бы строки экземпляра —
   * отказ не записан, попытка не потрачена, шаг повторяется до DEAD.
   * Ожидание строки — у РАННЕРА (его применяет транзакция), а не у оркестратора.
   */
  describe('потолок запроса шага — остаток бюджета шага', () => {
    const effects = pgTable('effects', {
      id: serial('id').primaryKey(),
      sagaId: text('saga_id').notNull(),
      step: text('step').notNull(),
      phase: text('phase').notNull(),
      attempt: integer('attempt').notNull(),
    });

    it.each(['execute', 'построитель'] as const)(
      'второй запрос (%s) начат за 100 мс до потолка шага 8 с: step-timed-out записан тем же проходом релея',
      async (via) => {
        let calls = 0;
        const definition = defineSaga<{ x: number }>(`late-${via === 'execute' ? 'e' : 'b'}`, { version: 1 })
          .step('a', {
            execute: async ({ tx, sagaId }) => {
              calls += 1;
              await tx.execute(sql`SELECT pg_sleep(7.9)`);
              if (via === 'execute') {
                await tx.execute(sql`SELECT pg_sleep(7.9)`);
              } else {
                await (tx as unknown as ReturnType<typeof drizzle>).insert(effects).values({
                  sagaId,
                  step: sql`(SELECT 'a' FROM pg_sleep(7.9))`,
                  phase: 'execute',
                  attempt: 1,
                });
              }
              return { a: 1 };
            },
            timeoutMs: 8_000,
            retry: { maxAttempts: 2, backoffMs: 1_000 },
          })
          .build();
        const pool = database.newPool(4);
        const replica = createReplica({
          pool,
          clock,
          definitions: [definition],
          transaction: () => sagaTransactionRunner(drizzle(pool), { lockTimeoutMs: 1_000 }),
        });
        await replica.start(definition, 'late-1', { x: 1 });

        await replica.relay.runOnce();

        expect({ calls, journal: await journalOf(replica, 'late-1') }).toEqual({
          calls: 1,
          journal: ['saga-started', 'step-timed-out:a'],
        });
        const { rows } = await database.pool.query<{ status: string; attempts: number }>(
          `SELECT status, attempts FROM ${TEST_TABLES.outbox}
            WHERE event_type = 'saga.command' AND payload->>'attempt' = '1'`,
        );
        expect(rows).toEqual([{ status: 'published', attempts: 0 }]);
        // На сервере поздний запрос не идёт: потолок снял его там же.
        const active = await database.pool.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM pg_stat_activity
            WHERE state = 'active' AND query LIKE '%pg_sleep(7.9)%' AND pid <> pg_backend_pid()`,
        );
        expect(active.rows[0]?.n).toBe(0);
      },
      30_000,
    );
  });
});
