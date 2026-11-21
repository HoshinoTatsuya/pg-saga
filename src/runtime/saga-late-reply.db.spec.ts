import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import { SagaStepRejectedError } from '../errors/saga-step-rejected.error';
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
  type TestDatabase,
  truncateSagaSchema,
} from '../testing/saga-test-database';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';

/**
 * Двойники детектора `late-reply` на НАСТОЯЩИХ историях: поздний дубль шага
 * (компенсации), уже выполненного другой попыткой или поколением, — не
 * событие ни у откатывающейся, ни у откатанной, ни у завершённой саги. Рядом —
 * то, что событием остаётся (успех, который сага не учла).
 */
describeDb('late-reply: дубли выполненного — не событие', () => {
  let database: TestDatabase;
  let clock: TestClock;
  let billing: BillingScript;

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
    billing = newBillingScript();
  });

  const passes = async (replica: SagaReplica, count: number): Promise<void> => {
    for (let pass = 0; pass < count; pass += 1) {
      await replica.relay.runOnce();
    }
  };

  const command = (phase: 'execute' | 'compensate', attempt: number, generation = 0): SagaCommandMeta => {
    const found = billing.received.find(
      (meta) => meta.phase === phase && meta.attempt === attempt && meta.generation === generation,
    );
    if (found === undefined) {
      throw new Error(`участник не получал ${phase} попытки ${attempt} поколения ${generation}`);
    }
    return found;
  };

  const lateReplies = async (replica: SagaReplica, id: string): Promise<string[]> =>
    (await journalOf(replica, id)).filter((event) => event.startsWith('late-reply'));

  it('откатывающаяся и откатанная сага: дубль пройденного шага и дубль выполненной компенсации — без записи', async () => {
    // reserve → charge (участник) → ship (окончательный отказ): откат после пройденного charge.
    const definition = defineSaga<{ orderId: string }>('late-twins', { version: 1 })
      .step('reserve', { execute: async () => ({ reservationId: 'r' }), compensate: async () => undefined })
      .remoteStep<{ chargeId: string }>('charge', {
        command: ({ stepKey }) => ({ eventType: 'billing.charge', payload: { stepKey } }),
        compensation: ({ executeStepKey }) => ({ eventType: 'billing.refund', payload: { executeStepKey } }),
        timeoutMs: 30_000,
        retry: { maxAttempts: 2, backoffMs: 1_000 },
        compensationRetry: { maxAttempts: 2, backoffMs: 1_000 },
      })
      .step('ship', {
        execute: async () => {
          throw new SagaStepRejectedError('доставки нет');
        },
      })
      .build();
    // Списание: попытка 1 молчит (таймаут), попытка 2 отвечает. Возврат — так же.
    billing.execute.push('silent', 'ok');
    billing.compensate.push('silent', 'ok');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'twins', { orderId: '1' });
    await passes(replica, 6);
    clock.now = new Date(clock.now.getTime() + 30_001);
    await passes(replica, 3);
    clock.now = new Date(clock.now.getTime() + 2_000);
    await passes(replica, 10);
    expect((await instanceOf(replica, 'twins')).status).toBe(SAGA_STATUS.compensating);

    // compensating | то же поколение | execute | шаг пройден попыткой 2.
    expect(
      await replica.orchestrator.handleReply(
        sagaReplyOf(command('execute', 1), { ok: true, data: { chargeId: 'x' } }),
      ),
    ).toBe(SAGA_HANDLED.stale);
    expect(await lateReplies(replica, 'twins')).toEqual([]);

    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'twins')).status).toBe(SAGA_STATUS.compensated);
    // compensated | то же поколение | compensate | возврат выполнен попыткой 2.
    expect(await replica.orchestrator.handleReply(sagaReplyOf(command('compensate', 1), { ok: true }))).toBe(
      SAGA_HANDLED.stale,
    );
    expect(await lateReplies(replica, 'twins')).toEqual([]);
  });

  it('завершённая сага: успех поколения ДО ручного повтора, шаг выполнен поколением после, — без записи', async () => {
    const definition = paymentSaga();
    billing.execute.push('silent', 'ok');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'older', { orderId: '1' });
    await passes(replica, 6);
    // Замёрз: таймер не доставлен, срок давно прошёл — ручной повтор.
    clock.now = new Date(clock.now.getTime() + 10 * 60_000);
    await replica.admin.retry('older', 'DEAD разобран', { stalledForMs: 60_000 });
    await passes(replica, 10);
    expect((await instanceOf(replica, 'older')).status).toBe(SAGA_STATUS.completed);

    expect(await replica.orchestrator.handleReply(sagaReplyOf(command('execute', 1, 0), { ok: true }))).toBe(
      SAGA_HANDLED.stale,
    );
    expect(await lateReplies(replica, 'older')).toEqual([]);
  });

  it('событие остаётся событием: успех, которого сага не учла (откат после таймаута), — запись late-reply', async () => {
    const definition = paymentSaga();
    billing.execute.push('silent', 'silent');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'real', { orderId: '1' });
    await pump(clock, [replica]);
    expect((await instanceOf(replica, 'real')).status).toBe(SAGA_STATUS.compensated);

    expect(await replica.orchestrator.handleReply(sagaReplyOf(command('execute', 2), { ok: true }))).toBe(
      SAGA_HANDLED.stale,
    );
    expect(await lateReplies(replica, 'real')).toEqual(['late-reply:charge']);
  });

  it('поздний успех КОМПЕНСАЦИИ шага, который был ВЫПОЛНЕН (step-completed есть), у застрявшего — запись late-reply', async () => {
    // Факт «компенсация выполнена» — step-compensated, а не step-completed: у
    // шага, выполненного успешно, step-completed есть всегда, и по нему поздний
    // возврат у stuck считался бы уже учтённым — ручной возврат стал бы вторым.
    const base = paymentSaga();
    const definition = {
      ...base,
      steps: base.steps.map((step) =>
        step.name === 'ship'
          ? {
              ...step,
              execute: async () => {
                throw new SagaStepRejectedError('отгрузка невозможна');
              },
            }
          : step,
      ),
    } as typeof base;
    billing.compensate.push('silent', 'silent');
    const replica = createReplica({
      pool: database.pool,
      clock,
      definitions: [definition],
      participants: billingParticipant(billing),
    });
    await replica.start(definition, 'done-then-stuck', { orderId: '1' });
    await pump(clock, [replica]);
    expect({
      status: (await instanceOf(replica, 'done-then-stuck')).status,
      journal: (await journalOf(replica, 'done-then-stuck')).filter((event) => event.endsWith(':charge')),
    }).toEqual({
      status: SAGA_STATUS.stuck,
      journal: [
        'step-dispatched:charge',
        'step-completed:charge',
        'step-dispatched:charge',
        'compensation-failed:charge',
        'step-dispatched:charge',
        'compensation-failed:charge',
      ],
    });

    expect(await replica.orchestrator.handleReply(sagaReplyOf(command('compensate', 2), { ok: true }))).toBe(
      SAGA_HANDLED.stale,
    );
    expect(await lateReplies(replica, 'done-then-stuck')).toEqual(['late-reply:charge']);
  });
});
