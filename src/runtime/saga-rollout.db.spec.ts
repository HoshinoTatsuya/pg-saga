import type { OutboxDeliveryPort } from '@hoshinotatsuya/outbox';

import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { sagaReplyOf } from '../remote/saga-reply-of';
import { parseSagaCommandV010, startAsV010 } from '../testing/legacy-v010';
import { createReplica, instanceOf, pump, START, type TestClock } from '../testing/saga-rig';
import {
  createSagaSchema,
  createTestDatabase,
  describeDb,
  TEST_TABLES,
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import { sagaMessageId } from './saga-message-id';

/**
 * Порядок выкатки 0.1.0 → 0.2.0. Участник с разборщиком 0.1.0 принимает только
 * ключ шага формата 1: экземплярам, начатым 0.1.0, оркестратор 0.2.0 шлёт
 * именно его (и id сообщений её формата), — летящие саги доезжают при любом
 * порядке. Экземпляру 0.2.0 ключ — с воплощением, и разборщик 0.1.0 отвергает
 * КАЖДУЮ его команду: поэтому участников обновляют ДО оркестратора.
 */
describeDb('выкатка: участник на разборщике 0.1.0 и оркестратор 0.2.0', () => {
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

  const definition = defineSaga<{ orderId: string }>('rollout', { version: 1 })
    .remoteStep<{ chargeId: string }>('charge', {
      command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
      compensation: ({ executeStepKey }) => ({ eventType: 'billing.refund', payload: { executeStepKey } }),
      timeoutMs: 60_000,
      retry: { maxAttempts: 1 },
    })
    .build();

  /** Участник 0.1.0: разбор её функцией, ответ — на каждую команду; отказ разбора — в `rejected`. */
  const legacyParticipant = (seen: string[], rejected: string[], answer: boolean) =>
    ((replica) => {
      const port: OutboxDeliveryPort = async (delivery) => {
        let meta: ReturnType<typeof parseSagaCommandV010>['meta'];
        try {
          ({ meta } = parseSagaCommandV010(delivery.payload));
        } catch (error) {
          rejected.push((error as Error).message);
          throw error;
        }
        seen.push(`${meta.phase} ${meta.stepKey}`);
        if (answer) {
          await replica.orchestrator.handleReply(
            sagaReplyOf(meta, { ok: true, data: meta.phase === 'execute' ? { chargeId: 'c' } : {} }),
          );
        }
      };
      return [
        ['billing.charge', port],
        ['billing.refund', port],
      ] as const;
    }) satisfies Parameters<typeof createReplica>[0]['participants'];

  it('экземпляр 0.1.0: выполнение и компенсация уходят с ключом формата 1 — участник 0.1.0 их разбирает, сага доезжает', async () => {
    const seen: string[] = [];
    const rejected: string[] = [];
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: legacyParticipant(seen, rejected, true),
    });
    await startAsV010(replica, definition, 'old-done', { orderId: '1' });
    await pump(clock, [replica]);
    // Компенсация: отмена после отправки команды — участник получит возврат.
    const silent = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: legacyParticipant(seen, rejected, false),
    });
    await startAsV010(silent, definition, 'old-cancel', { orderId: '2' });
    for (let pass = 0; pass < 4; pass += 1) {
      await silent.relay.runOnce();
    }
    await silent.transaction((tx) => silent.orchestrator.cancel(tx, 'old-cancel', 'клиент передумал'));
    for (let pass = 0; pass < 4; pass += 1) {
      await silent.relay.runOnce();
    }

    expect({
      rejected,
      seen,
      done: (await instanceOf(replica, 'old-done')).status,
      cancelled: (await instanceOf(replica, 'old-cancel')).status,
    }).toEqual({
      rejected: [],
      seen: [
        'execute old-done:execute:0',
        'execute old-cancel:execute:0',
        'compensate old-cancel:compensate:0',
      ],
      done: SAGA_STATUS.completed,
      // Ответа на возврат нет (участник молчит) — экземпляр ждёт его.
      cancelled: SAGA_STATUS.compensating,
    });
  });

  it('экземпляр 0.2.0: разборщик 0.1.0 отвергает каждую его команду, разборщик 0.2.0 — принимает (участники — до оркестратора)', async () => {
    const seen: string[] = [];
    const rejected: string[] = [];
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: legacyParticipant(seen, rejected, true),
    });
    await replica.start(definition, 'new-1', { orderId: '1' });
    for (let pass = 0; pass < 4; pass += 1) {
      await replica.relay.runOnce();
    }
    const { rows } = await database.pool.query<{ payload: unknown }>(
      `SELECT payload FROM ${TEST_TABLES.outbox} WHERE event_type = 'billing.charge'`,
    );
    const instance = await instanceOf(replica, 'new-1');
    expect({
      seen,
      rejected: [...new Set(rejected)],
      status: instance.status,
      current: parseSagaCommand(rows[0]?.payload).meta.stepKey,
    }).toEqual({
      seen: [],
      rejected: [
        `pg-saga: команда участнику: stepKey не сходится с адресом — new-1#${instance.incarnation}:execute:0`,
      ],
      status: SAGA_STATUS.running,
      current: `new-1#${instance.incarnation}:execute:0`,
    });
    expect(() => parseSagaCommandV010(rows[0]?.payload)).toThrow(SagaPayloadError);
  });

  /**
   * Старая админка или CLI (0.1.0) повторяет застрявший экземпляр 0.2.0: поднимает
   * поколение и пишет команду с id своего формата (без воплощения). Экземпляр
   * эту команду не примет (`stale`) — но и не промолчит: строка в логе называет
   * причину. Двойник — устаревшая команда с чужой попыткой: обычный `stale`
   * без этой строки.
   */
  it('команда ручного повтора от админки 0.1.0 экземпляру 0.2.0 — stale с предупреждением; чужая попытка — без него', async () => {
    const replica = createReplica({ pool: database.pool, clock, definitions: [definition] });
    await replica.start(definition, 'new-retry', { orderId: '1' });
    const instance = await instanceOf(replica, 'new-retry');
    // Что пишет SagaAdmin 0.1.0: новое поколение, попытка 1, id её формата.
    await database.pool.query(
      `UPDATE ${TEST_TABLES.instances} SET generation = 1, attempt = 1 WHERE id = 'new-retry'`,
    );
    const address = {
      sagaId: 'new-retry',
      generation: 1,
      phase: 'execute',
      stepIndex: 0,
      attempt: 1,
    } as const;
    const legacy = { ...address, commandId: sagaMessageId.command({ ...address, incarnation: null }) };
    const otherAttempt = {
      ...address,
      attempt: 2,
      commandId: sagaMessageId.command({ ...address, attempt: 2, incarnation: instance.incarnation }),
    };

    const handled = {
      legacy: await replica.orchestrator.handle('saga.command', legacy),
      otherAttempt: await replica.orchestrator.handle('saga.command', otherAttempt),
    };
    expect({ handled, warnings: replica.sagaWarnings }).toEqual({
      handled: { legacy: 'stale', otherAttempt: 'stale' },
      warnings: [
        expect.stringContaining(`команда ${legacy.commandId} с адресом, который ждёт экземпляр new-retry`),
      ],
    });
  });
});
