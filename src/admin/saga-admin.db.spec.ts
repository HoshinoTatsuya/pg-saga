import { setTimeout as delay } from 'node:timers/promises';

import { pgExecutor, withPgTransaction } from '@hoshinotatsuya/outbox/pg';
import { Pool } from 'pg';
import { runSagaStuckCli } from '../cli/run-saga-stuck-cli';
import type { SagaStuckCliIo } from '../cli/saga-stuck-cli-io.type';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaStateError } from '../errors/saga-state.error';
import {
  createReplica,
  effectsOf,
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
} from '../testing/saga-test-database';
import { type FailureScript, newScript, orderSaga } from '../testing/test-sagas';
import { SagaAdmin } from './saga-admin';

const MINUTE = 60_000;

/** Срезы для метрик, отчёт о застрявших и CLI ручного разбора — на настоящей базе. */
describeDb('SagaAdmin и CLI saga-stuck', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let script: FailureScript;
  let replica: SagaReplica;
  let definition: ReturnType<typeof orderSaga>;

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
    definition = orderSaga(script);
    replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
  });

  /** Застрявший экземпляр: charge отказал окончательно, компенсация reserve отказывает всегда. */
  const stuck = async (id: string): Promise<void> => {
    script.execute.set('charge', 'reject');
    script.compensate.set('reserve', 'always');
    await replica.start(definition, id, { orderId: id });
    await pump(clock, [replica]);
    script.execute.clear();
    script.compensate.clear();
  };

  it('stats: активные, застрявшие, начато и завершено с момента, возраст старейшего активного', async () => {
    await replica.start(definition, 'old-done', { orderId: '0' });
    await pump(clock, [replica]);
    clock.now = new Date(START.getTime() + 10 * MINUTE);
    const since = clock.now;
    await stuck('st-1');
    await replica.start(definition, 'done-1', { orderId: '1' });
    await pump(clock, [replica]);
    script.execute.set('reserve', 'reject');
    await replica.start(definition, 'comp-1', { orderId: '2' });
    await pump(clock, [replica]);
    script.execute.clear();
    await replica.start(definition, 'run-1', { orderId: '3' });
    const runStart = clock.now;
    clock.now = new Date(clock.now.getTime() + 5 * MINUTE);

    expect(await replica.admin.stats(since)).toEqual([
      {
        saga: 'order',
        blocked: 0,
        running: 1,
        compensating: 0,
        stuck: 1,
        started: 4,
        completed: 1,
        compensated: 1,
        resolved: 0,
        oldestActiveAgeMs: clock.now.getTime() - runStart.getTime(),
        longestIdleActiveMs: clock.now.getTime() - runStart.getTime(),
      },
    ]);
    expect(
      (await replica.admin.list({ status: SAGA_STATUS.completed })).map((item) => item.id).sort(),
    ).toEqual(['done-1', 'old-done']);
    expect((await replica.admin.list({ saga: 'нет такой' })).length).toBe(0);
  });

  it('prune: удаляет только завершённые старше срока, журнал — вместе с экземпляром', async () => {
    await replica.start(definition, 'p-old', { orderId: '1' });
    await pump(clock, [replica]);
    clock.now = new Date(clock.now.getTime() + 60 * MINUTE);
    await stuck('p-stuck');
    await replica.start(definition, 'p-new', { orderId: '2' });
    await pump(clock, [replica]);

    expect(await replica.admin.prune(new Date(clock.now.getTime() - 30 * MINUTE))).toBe(1);

    const { rows } = await database.pool.query<{ id: string }>(
      `SELECT id FROM ${TEST_TABLES.instances} ORDER BY id`,
    );
    expect(rows.map((row) => row.id)).toEqual(['p-new', 'p-stuck']);
    const journal = await database.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${TEST_TABLES.journal} WHERE saga_id = 'p-old'`,
    );
    expect(journal.rows[0]?.n).toBe(0);
  });

  it('prune: потолок пачки — на все завершённые статусы вместе; остаток уходит следующим вызовом', async () => {
    await replica.start(definition, 'z-done', { orderId: '1' });
    await pump(clock, [replica]);
    script.execute.set('reserve', 'reject');
    await replica.start(definition, 'z-comp', { orderId: '2' });
    await replica.start(definition, 'z-comp2', { orderId: '3' });
    await pump(clock, [replica]);
    script.execute.clear();
    await stuck('z-res');
    await replica.admin.resolve('z-res', 'разобран');
    clock.now = new Date(clock.now.getTime() + 60 * MINUTE);

    // 1 completed + 2 compensated + 1 resolved, пачка 2: второй статус получает
    // ОСТАТОК пачки (1), а не всю пачку — иначе вызов удалил бы 3.
    expect(await replica.admin.prune(clock.now, 2)).toBe(2);
    expect(await replica.admin.prune(clock.now, 2)).toBe(2);
    expect(await replica.admin.prune(clock.now, 2)).toBe(0);
  });

  it('stats считает разобранные вручную (resolved) за окно; две параллельные очистки не мешают друг другу', async () => {
    const since = clock.now;
    await stuck('r-1');
    await replica.admin.resolve('r-1', 'возврат проведён вручную');
    expect((await replica.admin.stats(since))[0]?.resolved).toBe(1);

    for (const id of ['q-1', 'q-2', 'q-3', 'q-4']) {
      await replica.start(definition, id, { orderId: id });
    }
    await pump(clock, [replica]);
    clock.now = new Date(clock.now.getTime() + 60 * MINUTE);
    const counts = await Promise.all([replica.admin.prune(clock.now, 3), replica.admin.prune(clock.now, 3)]);
    expect(counts[0] + counts[1] + (await replica.admin.prune(clock.now))).toBe(5);
  });

  it('stalledReport и повтор замёрзшего: активный, не двигавшийся дольше порога, виден и повторяется только явно', async () => {
    // Доставка, ушедшая в DEAD на отказе инфраструктуры, оставляет экземпляр
    // running навсегда: в stuckReport его нет. Здесь «замерзание» — команда,
    // которую никто не доставил.
    await replica.start(definition, 'frozen', { orderId: 'f' });
    const started = clock.now;
    clock.now = new Date(started.getTime() + 5 * MINUTE);

    expect(
      (await replica.admin.stalledReport(4 * MINUTE)).map((item) => [item.id, item.step, item.status]),
    ).toEqual([['frozen', 'reserve', 'running']]);
    expect(await replica.admin.stalledReport(6 * MINUTE)).toEqual([]);
    expect(await replica.admin.stuckReport()).toEqual([]);
    await expect(replica.admin.retry('frozen')).rejects.toThrow(SagaStateError);
    await expect(replica.admin.retry('frozen', null, { stalledForMs: 6 * MINUTE })).rejects.toThrow(
      SagaStateError,
    );

    await replica.admin.retry('frozen', 'доставка ушла в DEAD', { stalledForMs: 4 * MINUTE });
    await pump(clock, [replica]);

    const shown = await replica.admin.show('frozen');
    expect({
      status: shown?.instance.status,
      retry: shown?.journal.find((entry) => entry.event === 'manual-retry'),
      effects: await effectsOf(database.pool, 'frozen'),
    }).toEqual({
      status: SAGA_STATUS.completed,
      retry: expect.objectContaining({ phase: 'execute', step: 'reserve', generation: 1 }),
      effects: ['reserve:execute', 'notify:execute', 'charge:execute', 'ship:execute'],
    });
  });

  it('retry с поправкой контекста: поля поверх прежних, ключи — в журнал; хуки ручных действий — после фиксации', async () => {
    const hooks: string[] = [];
    const admin = new SagaAdmin({
      db: replica.db,
      transaction: replica.transaction,
      outbox: replica.outbox,
      tables: { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal },
      commandEventType: 'saga.command',
      clock: () => clock.now,
      metrics: {
        manualRetry: (event) =>
          hooks.push(`retry:${event.sagaId}:${String(event.contextPatched)}:${String(event.stalled)}`),
        manualResolve: (event) => hooks.push(`resolve:${event.sagaId}`),
      },
    });
    await stuck('p-patch');
    await expect(admin.retry('p-patch', null, { contextPatch: { amount: 1n } as never })).rejects.toThrow();
    expect(hooks).toEqual([]);

    await admin.retry('p-patch', 'сумма исправлена', { contextPatch: { orderId: 'исправлен' } });
    const shown = await admin.show('p-patch');
    expect({
      orderId: shown?.instance.context.orderId,
      detail: shown?.journal.find((entry) => entry.event === 'manual-retry')?.detail,
    }).toEqual({ orderId: 'исправлен', detail: 'сумма исправлена; контекст поправлен: orderId' });

    await stuck('p-res');
    await admin.resolve('p-res', 'вручную');
    expect(hooks).toEqual(['retry:p-patch:true:false', 'resolve:p-res']);
  });

  it('часы разбора позади записи (её сделала реплика с часами впереди): stuckForMs и возраст старейшего — 0, а не минус; двойник — верные часы', async () => {
    await stuck('skew-stuck');
    await replica.start(definition, 'skew-active', { orderId: 'a' });
    const behind = new Date(START.getTime() - 10 * MINUTE);
    const after = new Date(clock.now.getTime() + 2 * MINUTE);
    const stuckFor = async (now: Date): Promise<number[]> =>
      (await replica.admin.stuckReport(10, now)).map((item) => item.stuckForMs);
    const age = async (now: Date): Promise<Array<number | null>> =>
      (await replica.admin.stats(new Date(now.getTime() - MINUTE), now)).map((row) => row.oldestActiveAgeMs);
    const { rows } = await database.pool.query<{ updated: Date; created: Date }>(
      `SELECT (SELECT updated_at FROM ${TEST_TABLES.instances} WHERE id = 'skew-stuck') AS updated,
              (SELECT created_at FROM ${TEST_TABLES.instances} WHERE id = 'skew-active') AS created`,
    );
    const updated = rows[0]?.updated.getTime() ?? Number.NaN;
    const created = rows[0]?.created.getTime() ?? Number.NaN;

    expect({
      behind: { stuckFor: await stuckFor(behind), age: await age(behind) },
      after: { stuckFor: await stuckFor(after), age: await age(after) },
    }).toEqual({
      behind: { stuckFor: [0], age: [0] },
      after: { stuckFor: [after.getTime() - updated], age: [after.getTime() - created] },
    });
    expect(after.getTime() - updated).toBeGreaterThan(0);
  });

  it('retry без типа команды оркестратора — отказ до записи: экземпляр остаётся stuck, команд не прибавилось', async () => {
    await stuck('no-type');
    const before = await database.pool.query(`SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox}`);
    const admin = new SagaAdmin({
      db: replica.db,
      transaction: replica.transaction,
      outbox: replica.outbox,
      tables: { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal },
    });

    await expect(admin.retry('no-type')).rejects.toThrow(SagaConfigError);
    // Без outbox — тоже отказ до записи: команде некуда писаться.
    const noOutbox = new SagaAdmin({
      db: replica.db,
      transaction: replica.transaction,
      tables: { instances: TEST_TABLES.instances, journal: TEST_TABLES.journal },
      commandEventType: 'saga.command',
    });
    await expect(noOutbox.retry('no-type')).rejects.toThrow(/outbox/);

    const after = await database.pool.query(`SELECT count(*)::int AS n FROM ${TEST_TABLES.outbox}`);
    expect({ status: (await instanceOf(replica, 'no-type')).status, outbox: after.rows[0] }).toEqual({
      status: SAGA_STATUS.stuck,
      outbox: before.rows[0],
    });
  });

  it('stalledReport: завершённые и застрявшие, сколь угодно старые, — не замёрзшие; только активные', async () => {
    script.execute.set('notify', 'reject');
    await replica.start(definition, 'done', { orderId: 'd' });
    await pump(clock, [replica]);
    script.execute.clear();
    await stuck('stuck');
    await replica.start(definition, 'idle', { orderId: 'i' });
    clock.now = new Date(clock.now.getTime() + 60 * MINUTE);
    expect([(await instanceOf(replica, 'done')).status, (await instanceOf(replica, 'stuck')).status]).toEqual(
      [SAGA_STATUS.compensated, SAGA_STATUS.stuck],
    );

    expect((await replica.admin.stalledReport(MINUTE)).map((item) => item.id)).toEqual(['idle']);
  });

  it('ждущий паузы повтора — не замёрзший до её срока (wake_at), после — замёрзший', async () => {
    // reserve отказывает один раз: повтор через паузу (backoff 1000 мс с половиной случайной).
    script.execute.set('reserve', 1);
    await replica.start(definition, 'pause', { orderId: 'p' });
    await replica.relay.runOnce();
    const instance = await instanceOf(replica, 'pause');
    const wakeAt = instance.wakeAt?.getTime() ?? 0;
    expect({ attempt: instance.attempt, pauseMs: wakeAt - instance.updatedAt.getTime() }).toEqual({
      attempt: 2,
      pauseMs: 750,
    });
    clock.now = new Date(wakeAt - 1);
    expect(await replica.admin.stalledReport(1)).toEqual([]);
    clock.now = new Date(wakeAt + 2 * MINUTE);
    expect((await replica.admin.stalledReport(MINUTE)).map((item) => item.id)).toEqual(['pause']);
  });

  it('stalledReport — старейшие по ПОСЛЕДНЕМУ ДВИЖЕНИЮ первыми, а не по updated_at', async () => {
    await replica.start(definition, 'moved-early', { orderId: 'a' });
    clock.now = new Date(clock.now.getTime() + MINUTE);
    await replica.start(definition, 'moved-late', { orderId: 'b' });
    // Сдвинулся раньше, но ждёт срока дольше: простаивает меньше.
    await database.pool.query(`UPDATE ${TEST_TABLES.instances} SET wake_at = $1 WHERE id = 'moved-early'`, [
      new Date(clock.now.getTime() + 5 * MINUTE),
    ]);
    clock.now = new Date(clock.now.getTime() + 30 * MINUTE);

    expect(
      (await replica.admin.stalledReport(MINUTE)).map((item) => [
        item.id,
        Math.round(item.stuckForMs / MINUTE),
      ]),
    ).toEqual([
      ['moved-late', 30],
      ['moved-early', 25],
    ]);
  });

  it('порог простоя дольше int4 мс (30 суток) — принимается API; сверх общего потолка — отказ', async () => {
    const days30 = 30 * 24 * 60 * MINUTE;
    await replica.start(definition, 'old', { orderId: 'o' });
    clock.now = new Date(clock.now.getTime() + days30 + MINUTE);
    expect((await replica.admin.stalledReport(days30)).map((item) => item.id)).toEqual(['old']);
    await replica.admin.retry('old', 'месяц без движения', { stalledForMs: days30 });
    expect((await instanceOf(replica, 'old')).generation).toBe(1);
    await expect(replica.admin.stalledReport(10 * 365 * 24 * 60 * MINUTE + 1)).rejects.toThrow(
      SagaConfigError,
    );
  });

  it('resolve живого running без stalledForMs (и с порогом больше простоя) — отказ, экземпляр не тронут; с порогом — разобран', async () => {
    await replica.start(definition, 'live', { orderId: 'l' });
    clock.now = new Date(clock.now.getTime() + 5 * MINUTE);
    await expect(replica.admin.resolve('live', 'вручную')).rejects.toThrow(SagaStateError);
    await expect(replica.admin.resolve('live', 'вручную', { stalledForMs: 6 * MINUTE })).rejects.toThrow(
      SagaStateError,
    );
    expect((await instanceOf(replica, 'live')).status).toBe(SAGA_STATUS.running);
    // Двойник: замёрзший дольше порога — разбирается.
    await replica.admin.resolve('live', 'доставка в DEAD, разобрано', { stalledForMs: 4 * MINUTE });
    expect((await instanceOf(replica, 'live')).status).toBe(SAGA_STATUS.resolved);
  });

  it('порог простоя 0 или меньше — отказ API у retry, resolve и stalledReport: иначе живой шаг запустился бы второй раз', async () => {
    await replica.start(definition, 'zero', { orderId: 'z' });
    for (const threshold of [0, -1]) {
      await expect(replica.admin.retry('zero', null, { stalledForMs: threshold })).rejects.toThrow(
        SagaConfigError,
      );
      await expect(replica.admin.resolve('zero', 'x', { stalledForMs: threshold })).rejects.toThrow(
        SagaConfigError,
      );
      await expect(replica.admin.stalledReport(threshold)).rejects.toThrow(SagaConfigError);
    }
    const instance = await instanceOf(replica, 'zero');
    expect({ status: instance.status, generation: instance.generation }).toEqual({
      status: SAGA_STATUS.running,
      generation: 0,
    });
    // Двойник: порог 1 мс — допустимое значение (простой 2 мс больше порога).
    clock.now = new Date(clock.now.getTime() + 2);
    expect((await replica.admin.stalledReport(1)).map((item) => item.id)).toEqual(['zero']);
  });

  it('retry с пустой или пробельной причиной — в журнале её нет, а не «отказ без описания»', async () => {
    await stuck('blank');
    await replica.admin.retry('blank', '   ');
    const shown = await replica.admin.show('blank');
    expect(
      shown?.journal.filter((entry) => entry.event === 'manual-retry').map((entry) => entry.detail),
    ).toEqual([null]);
  });

  describe('CLI', () => {
    let out: string[];
    let err: string[];
    let pools: Pool[];

    const io = (
      env: Record<string, string | undefined> = { SAGA_DATABASE_URL: database.url },
    ): SagaStuckCliIo => ({
      env,
      stdout: (line) => out.push(line),
      stderr: (line) => err.push(line),
      connect: async (url) => {
        const pool = new Pool({ connectionString: url, max: 1 });
        pools.push(pool);
        return {
          executor: pgExecutor(pool),
          transaction: (work) => withPgTransaction(pool, work),
          close: () => pool.end(),
        };
      },
    });
    const TABLES = ['--instances', TEST_TABLES.instances, '--journal', TEST_TABLES.journal];
    const RETRY = [
      ...TABLES,
      '--outbox',
      TEST_TABLES.outbox,
      '--command-type',
      'saga.command',
      '--id',
      'x',
      '--yes',
    ];

    beforeEach(() => {
      out = [];
      err = [];
      pools = [];
    });

    afterEach(async () => {
      await Promise.all(pools.map((pool) => pool.end().catch(() => undefined)));
    });

    it('неверный вызов — код 2 и ни одного подключения', async () => {
      const cases: string[][] = [
        [],
        ['drop', ...TABLES],
        ['list'],
        ['show', ...TABLES],
        ['resolve', ...TABLES, '--id', 'x'],
        ['retry', ...TABLES, '--id', 'x'],
        // Без типа команды: угаданный saga.command при своих eventTypes ушёл бы в DEAD.
        ['retry', ...TABLES, '--outbox', TEST_TABLES.outbox, '--id', 'x', '--yes'],
        ['list', ...TABLES, '--limit', '0'],
        ['list', ...TABLES, '--stalled-ms', '0'],
        // Целые — только цифрами: `1e3` и `0x10` Number() принял бы.
        ['list', ...TABLES, '--limit', '1e3'],
        ['list', ...TABLES, '--limit', '0x10'],
        ['list', ...TABLES, '--limit', '10001'],
        ['prune', ...TABLES, '--yes'],
        ['list', ...TABLES, '--unknown'],
        // Поправка контекста: только у retry, одна из двух форм, JSON-объект с полями.
        ['list', ...TABLES, '--context-patch', '{"a":1}'],
        ['retry', ...RETRY, '--context-patch', '{"a":1}', '--context-patch-file', '/x.json'],
        ['retry', ...RETRY, '--context-patch', '{"a":'],
        ['retry', ...RETRY, '--context-patch', '[1]'],
        ['retry', ...RETRY, '--context-patch', '{}'],
        ['retry', ...RETRY, '--context-patch', '{"a":"\\u0000"}'],
        ['retry', ...RETRY, '--context-patch-file', '/нет/такого/файла.json'],
        // Пустой id и неверные имена — до подключения, а не отказом базы с кодом 1.
        ['show', ...TABLES, '--id', ''],
        ['list', '--instances', 'Bad-Name', '--journal', TEST_TABLES.journal],
        ['list', ...TABLES, '--schema', 'x y'],
        ['retry', ...TABLES, '--outbox', 'out;box', '--command-type', 'saga.command', '--id', 'x'],
        ['retry', ...TABLES, '--outbox', TEST_TABLES.outbox, '--command-type', 'x'.repeat(65), '--id', 'x'],
      ];
      for (const argv of cases) {
        expect(await runSagaStuckCli(argv, io())).toBe(2);
      }
      expect(await runSagaStuckCli(['list', ...TABLES], io({}))).toBe(2);
      expect(pools).toHaveLength(0);
      expect(err).toHaveLength(cases.length + 1);
    });

    it('list → show → retry без --yes ничего не меняет → retry --yes ставит компенсацию заново', async () => {
      await stuck('c-1');

      expect(await runSagaStuckCli(['list', ...TABLES], io())).toBe(0);
      expect(JSON.parse(out[0] ?? '{}')).toEqual(
        expect.objectContaining({ id: 'c-1', step: 'reserve', saga: 'order' }),
      );
      expect(out.at(-1)).toBe('застрявших: 1');

      out = [];
      expect(
        await runSagaStuckCli(
          [
            'retry',
            ...TABLES,
            '--outbox',
            TEST_TABLES.outbox,
            '--command-type',
            'saga.command',
            '--id',
            'c-1',
          ],
          io(),
        ),
      ).toBe(0);
      expect(out.at(-1)).toBe('retry: ничего не изменено — добавьте --yes');
      expect((await instanceOf(replica, 'c-1')).status).toBe(SAGA_STATUS.stuck);

      expect(
        await runSagaStuckCli(
          [
            'retry',
            ...TABLES,
            '--outbox',
            TEST_TABLES.outbox,
            '--command-type',
            'saga.command',
            '--id',
            'c-1',
            '--note',
            'склад поднят',
            '--yes',
          ],
          io(),
        ),
      ).toBe(0);
      await pump(clock, [replica]);

      const done = await replica.admin.show('c-1');
      expect({
        status: done?.instance.status,
        retry: done?.journal.find((e) => e.event === 'manual-retry')?.detail,
      }).toEqual({
        status: SAGA_STATUS.compensated,
        retry: 'склад поднят',
      });
    });

    it('retry --context-patch: без --yes — только ключи поправки; с --yes — поля поверх прежних, ключи в журнале; из файла — то же', async () => {
      await stuck('c-patch');
      await stuck('c-file');
      const retry = (id: string, ...rest: string[]): string[] => [
        'retry',
        ...TABLES,
        '--outbox',
        TEST_TABLES.outbox,
        '--command-type',
        'saga.command',
        '--id',
        id,
        ...rest,
      ];

      expect(
        await runSagaStuckCli(retry('c-patch', '--context-patch', '{"orderId":"секрет-42"}'), io()),
      ).toBe(0);
      expect({
        tail: out.slice(-2),
        leaked: out.some((line) => line.includes('секрет-42')),
        context: (await instanceOf(replica, 'c-patch')).context.orderId,
      }).toEqual({
        tail: ['retry: поправка контекста — поля orderId', 'retry: ничего не изменено — добавьте --yes'],
        leaked: false,
        context: 'c-patch',
      });

      expect(
        await runSagaStuckCli(retry('c-patch', '--context-patch', '{"orderId":"исправлен"}', '--yes'), io()),
      ).toBe(0);
      const reads: string[] = [];
      expect(
        await runSagaStuckCli(
          retry('c-file', '--context-patch-file', '/патч.json', '--note', 'сумма', '--yes'),
          {
            ...io(),
            readFile: async (path) => {
              reads.push(path);
              return '{"orderId":"из файла","extra":true}';
            },
          },
        ),
      ).toBe(0);
      const journal = async (id: string): Promise<string | null | undefined> =>
        (await replica.admin.show(id))?.journal.find((entry) => entry.event === 'manual-retry')?.detail;
      expect({
        patched: (await instanceOf(replica, 'c-patch')).context,
        patchedJournal: await journal('c-patch'),
        file: (await instanceOf(replica, 'c-file')).context,
        fileJournal: await journal('c-file'),
        reads,
        err,
      }).toEqual({
        // Поля поверх прежних: reservationId шага reserve на месте.
        patched: { orderId: 'исправлен', reservationId: 'r-c-patch' },
        patchedJournal: 'контекст поправлен: orderId',
        file: { orderId: 'из файла', extra: true, reservationId: 'r-c-file' },
        fileJournal: 'сумма; контекст поправлен: orderId, extra',
        reads: ['/патч.json'],
        err: [],
      });
    });

    it('list --stalled-ms — замёрзшие активные; retry --stalled-ms повторяет шаг в его фазе', async () => {
      await replica.start(definition, 'cli-frozen', { orderId: 'z' });
      clock.now = new Date(clock.now.getTime() + 5 * MINUTE);

      expect(await runSagaStuckCli(['list', ...TABLES, '--stalled-ms', String(4 * MINUTE)], io())).toBe(0);
      expect(JSON.parse(out[0] ?? '{}')).toEqual(
        expect.objectContaining({ id: 'cli-frozen', step: 'reserve' }),
      );
      expect(out.at(-1)).toBe('замёрзших: 1');

      expect(
        await runSagaStuckCli(
          [
            'retry',
            ...TABLES,
            '--outbox',
            TEST_TABLES.outbox,
            '--command-type',
            'saga.command',
            '--id',
            'cli-frozen',
            '--stalled-ms',
            String(4 * MINUTE),
            '--yes',
          ],
          io(),
        ),
      ).toBe(0);
      await pump(clock, [replica]);
      expect((await instanceOf(replica, 'cli-frozen')).status).toBe(SAGA_STATUS.completed);
    });

    it('resolve, пока строку экземпляра держит шаг: код 1 по ожиданию блокировки (lock_timeout), а не вечное ожидание', async () => {
      await stuck('cli-held');
      const holder = await database.pool.connect();
      let outcome: number | string;
      try {
        await holder.query('BEGIN');
        await holder.query(`SELECT 1 FROM ${TEST_TABLES.instances} WHERE id = 'cli-held' FOR UPDATE`);
        const run = runSagaStuckCli(
          ['resolve', ...TABLES, '--id', 'cli-held', '--note', 'вручную', '--yes'],
          io(),
        );
        // Потолок ожидания — 5 с; без него вызов висел бы, пока строку не отпустят.
        const limit = new AbortController();
        outcome = await Promise.race([
          run,
          delay(9_000, 'висит за блокировкой', { signal: limit.signal }).catch(() => 'снят'),
        ]);
        limit.abort();
        await holder.query('ROLLBACK');
        await run;
      } finally {
        holder.release();
      }
      expect({ outcome, lockTimeout: err.some((line) => line.includes('55P03')) }).toEqual({
        outcome: 1,
        lockTimeout: true,
      });
    });

    it('--help — справка с кодом 0; stats — срез JSON; prune без --yes ничего не удаляет, с --yes — удаляет', async () => {
      expect(await runSagaStuckCli(['--help'], io())).toBe(0);
      expect(out.at(-1)).toContain('saga-stuck <list|show|retry|resolve|stats|prune>');

      await replica.start(definition, 'cli-done', { orderId: 'd' });
      await pump(clock, [replica]);
      out = [];
      expect(
        await runSagaStuckCli(['stats', ...TABLES, '--since-ms', String(365 * 24 * 60 * MINUTE)], io()),
      ).toBe(0);
      expect(JSON.parse(out[0] ?? '{}')).toEqual(expect.objectContaining({ saga: 'order', completed: 1 }));

      out = [];
      expect(await runSagaStuckCli(['prune', ...TABLES, '--older-than-ms', '1'], io())).toBe(0);
      expect(out.at(-1)).toContain('ничего не удалено');
      expect((await replica.admin.list({ saga: 'order' })).length).toBe(1);
      expect(await runSagaStuckCli(['prune', ...TABLES, '--older-than-ms', '1', '--yes'], io())).toBe(0);
      expect(out.at(-1)).toBe('prune: удалено 1');
    });

    it('resolve --yes помечает разобранным; повтор над не-stuck — код 1 с причиной', async () => {
      await stuck('c-2');

      expect(
        await runSagaStuckCli(['resolve', ...TABLES, '--id', 'c-2', '--note', 'вручную', '--yes'], io()),
      ).toBe(0);
      expect((await instanceOf(replica, 'c-2')).status).toBe(SAGA_STATUS.resolved);

      expect(
        await runSagaStuckCli(
          [
            'retry',
            ...TABLES,
            '--outbox',
            TEST_TABLES.outbox,
            '--command-type',
            'saga.command',
            '--id',
            'c-2',
            '--yes',
          ],
          io(),
        ),
      ).toBe(1);
      expect(err.at(-1)).toContain('в состоянии resolved');
      // Без --yes — тоже отказ с причиной, а не «добавьте --yes» тому, на чём --yes откажет.
      out = [];
      expect(
        await runSagaStuckCli(
          [
            'retry',
            ...TABLES,
            '--outbox',
            TEST_TABLES.outbox,
            '--command-type',
            'saga.command',
            '--id',
            'c-2',
          ],
          io(),
        ),
      ).toBe(1);
      expect(out.some((line) => line.includes('добавьте --yes'))).toBe(false);
      expect(await runSagaStuckCli(['resolve', ...TABLES, '--id', 'c-2', '--note', 'ещё раз'], io())).toBe(1);
      expect(err.at(-1)).toContain('в состоянии resolved');
      expect(await runSagaStuckCli(['show', ...TABLES, '--id', 'нет'], io())).toBe(1);
    });

    it('--stalled-ms 30 суток (длиннее int4 мс) — код 0 и отчёт; сверх общего потолка — код 2 без подключения', async () => {
      const days30 = 30 * 24 * 60 * MINUTE;
      await replica.start(definition, 'month', { orderId: 'm' });
      // CLI берёт настенные часы: «давно» — по часам сервера, а не процесса спеки.
      await database.pool.query(
        `UPDATE ${TEST_TABLES.instances} SET updated_at = now() - interval '30 days 1 minute'`,
      );
      expect(await runSagaStuckCli(['list', ...TABLES, '--stalled-ms', String(days30)], io())).toBe(0);
      expect(
        out.map((line) => (line.startsWith('{') ? (JSON.parse(line) as { id: string }).id : line)),
      ).toEqual(['month', 'замёрзших: 1']);
      expect(
        await runSagaStuckCli(
          ['list', ...TABLES, '--stalled-ms', String(10 * 365 * 24 * 60 * MINUTE + 1)],
          io(),
        ),
      ).toBe(2);
      expect(pools).toHaveLength(1);
    });
  });
});
