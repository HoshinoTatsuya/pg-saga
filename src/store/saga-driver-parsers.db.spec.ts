import { types } from 'pg';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { createReplica, instanceOf, pump, START, type TestClock } from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import { newScript, orderSaga } from '../testing/test-sagas';

/**
 * Приложение вправе переопределить ГЛОБАЛЬНЫЙ парсер timestamptz драйвера
 * (строки, luxon, dayjs): чтение экземпляра и журнала от этого не зависит.
 * Плюс форма данных ручного повтора: поправка контекста — объект.
 */
describeDb('чтение саги при чужих парсерах драйвера и форма поправки контекста', () => {
  let database: TestDatabase;
  let clock: TestClock;
  const original = types.getTypeParser(types.builtins.TIMESTAMPTZ);

  beforeAll(async () => {
    database = await createTestDatabase();
    await createSagaSchema(database.pool);
  });

  afterAll(async () => {
    types.setTypeParser(types.builtins.TIMESTAMPTZ, original);
    await database?.drop();
  });

  beforeEach(async () => {
    await truncateSagaSchema(database.pool);
    clock = { now: new Date(START) };
  });

  it.each([
    ['строка как есть', (text: string) => text],
    ['объект-обёртка с toJSON (как luxon/dayjs)', (text: string) => ({ iso: text, toJSON: () => text })],
  ])('глобальный парсер timestamptz: %s — экземпляр, журнал и срез читаются', async (_name, parser) => {
    const definition = orderSaga(newScript());
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'parsers', { orderId: '1' });
    types.setTypeParser(types.builtins.TIMESTAMPTZ, parser as never);
    try {
      const shown = await replica.admin.show('parsers');
      const stats = await replica.admin.stats(new Date(START.getTime() - 1));
      expect({
        createdAt: shown?.instance.createdAt.toISOString(),
        journalAt: shown?.journal.map((entry) => entry.at.toISOString()),
        oldest: stats[0]?.oldestActiveAgeMs,
      }).toEqual({ createdAt: START.toISOString(), journalAt: [START.toISOString()], oldest: 0 });
    } finally {
      types.setTypeParser(types.builtins.TIMESTAMPTZ, original);
    }
  });

  it('retry с поправкой контекста — Date (toJSON отдаёт строку): отказ до записи, а не ключи «0»…«23» в контексте', async () => {
    const script = newScript();
    script.execute.set('charge', 'reject');
    script.compensate.set('reserve', 'always');
    const definition = orderSaga(script);
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'patch', { orderId: '1' });
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'patch')).status).toBe(SAGA_STATUS.stuck);
    const before = (await instanceOf(replica, 'patch')).context;

    await expect(
      replica.admin.retry('patch', 'n', { contextPatch: new Date('2026-11-01T00:00:00Z') as never }),
    ).rejects.toThrow(SagaPayloadError);
    expect((await instanceOf(replica, 'patch')).context).toEqual(before);
  });
});
