import { SAGA_STATUS } from '../constants/saga-status.constant';
import { defineSaga } from '../definition/define-saga';
import type { SagaInstance } from '../types/saga-instance.type';
import { SagaTransitions } from './saga-transitions';

const noop = async (): Promise<void> => undefined;

/** a (компенсация), b (нет), c (компенсация), d удалённый (компенсация), e (нет). */
const definition = defineSaga<{ id: string }>('t', { version: 1 })
  .step('a', { execute: noop, compensate: noop, retry: { maxAttempts: 2, backoffMs: 100 } })
  .step('b', { execute: noop })
  .step('c', { execute: noop, compensate: noop })
  .remoteStep('d', {
    command: () => ({ eventType: 'x', payload: {} }),
    compensation: () => ({ eventType: 'y', payload: {} }),
    timeoutMs: 1_000,
    compensationRetry: { maxAttempts: 2, backoffMs: 100 },
  })
  .step('e', { execute: noop })
  .build();

const NOW = new Date('2026-11-01T12:00:00.000Z');

const instance = (patch: Partial<SagaInstance>): SagaInstance => ({
  id: 's',
  name: 't',
  version: 1,
  steps: definition.steps.map((step) => step.name),
  status: SAGA_STATUS.running,
  stepIndex: 0,
  attempt: 1,
  incarnation: 7,
  generation: 0,
  context: { id: 's' },
  failure: null,
  lastError: null,
  createdAt: new Date(NOW.getTime() - 5_000),
  updatedAt: NOW,
  finishedAt: null,
  wakeAt: null,
  ...patch,
});

/** Прогнать компенсации до конца: успех каждой — следующий шаг. Порядок шагов компенсации. */
const compensationOrder = (start: SagaInstance): string[] => {
  const order: string[] = [];
  let current = start;
  for (let guard = 0; current.status === SAGA_STATUS.compensating && guard < 10; guard += 1) {
    order.push(definition.steps[current.stepIndex]?.name ?? '?');
    current = { ...current, ...SagaTransitions.success(definition, current, {}, NOW).state };
  }
  expect(current.status).toBe(SAGA_STATUS.compensated);
  return order;
};

const definite = { error: 'нет', retryable: false, uncertain: false, timedOut: false };

describe('SagaTransitions', () => {
  it.each([
    [0, []],
    [1, ['a']],
    [2, ['a']],
    [3, ['c', 'a']],
    [4, ['d', 'c', 'a']],
  ])(
    'окончательный отказ шага %i: компенсации завершённых с компенсацией — в обратном порядке',
    (index, order) => {
      const failed = SagaTransitions.failure(
        definition,
        instance({ stepIndex: index }),
        definite,
        NOW,
        () => 0.5,
      );
      const after = { ...instance({ stepIndex: index }), ...failed.state };

      expect(after.status === SAGA_STATUS.compensated ? [] : compensationOrder(after)).toEqual(order);
      expect(failed.state.failure).toBe('нет');
    },
  );

  it('исход неизвестен (таймаут удалённого шага): компенсируется и сам шаг', () => {
    const failed = SagaTransitions.failure(
      definition,
      instance({ stepIndex: 3, attempt: 3 }),
      { error: 'таймаут', retryable: true, uncertain: true, timedOut: true },
      NOW,
      () => 0.5,
    );

    expect(compensationOrder({ ...instance({ stepIndex: 3 }), ...failed.state })).toEqual(['d', 'c', 'a']);
    expect(failed.journal.map((entry) => entry.event)).toEqual(['step-timed-out']);
  });

  it('повтор: попытка +1, пауза по backoff, команда той же фазы и шага', () => {
    const failed = SagaTransitions.failure(
      definition,
      instance({ stepIndex: 0, attempt: 1 }),
      { ...definite, retryable: true },
      NOW,
      () => 1,
    );

    expect({ state: failed.state.attempt, command: failed.command }).toEqual({
      state: 2,
      command: {
        phase: 'execute',
        stepIndex: 0,
        attempt: 2,
        generation: 0,
        delayMs: 100,
      },
    });
  });

  it('отказ компенсации: повтор, по исчерпании — stuck на том же шаге и без команды', () => {
    const compensating = instance({ status: SAGA_STATUS.compensating, stepIndex: 3, attempt: 1 });
    const retried = SagaTransitions.failure(
      definition,
      compensating,
      { ...definite, retryable: true },
      NOW,
      () => 0,
    );
    const exhausted = SagaTransitions.failure(
      definition,
      { ...compensating, attempt: 2 },
      { ...definite, retryable: true },
      NOW,
      () => 0,
    );

    expect(retried.command).toEqual(
      expect.objectContaining({ phase: 'compensate', stepIndex: 3, attempt: 2 }),
    );
    expect({
      status: exhausted.state.status,
      stepIndex: exhausted.state.stepIndex,
      command: exhausted.command,
    }).toEqual({
      status: SAGA_STATUS.stuck,
      stepIndex: 3,
      command: null,
    });
    expect(exhausted.signals.map((signal) => signal.kind)).toEqual(['stepFailed', 'stuck']);
  });

  it('успех последнего шага — completed с длительностью; успех среднего — команда следующего, контекст влит', () => {
    const last = SagaTransitions.success(definition, instance({ stepIndex: 4 }), { z: 1 }, NOW);
    const middle = SagaTransitions.success(definition, instance({ stepIndex: 1, attempt: 2 }), { y: 2 }, NOW);

    expect(last.state.status).toBe(SAGA_STATUS.completed);
    expect(last.signals).toEqual([
      {
        kind: 'stepCompleted',
        event: { saga: 't', sagaId: 's', step: 'e', phase: 'execute', attempt: 1, sinceLastMoveMs: 0 },
      },
      { kind: 'completed', event: { saga: 't', sagaId: 's', durationMs: 5_000 } },
    ]);
    expect({ state: middle.state, command: middle.command }).toEqual({
      state: expect.objectContaining({ stepIndex: 2, attempt: 1, context: { id: 's', y: 2 } }),
      command: { phase: 'execute', stepIndex: 2, attempt: 1, generation: 0, delayMs: null },
    });
  });

  it('текст отказа нормализуется в самом переходе: NUL вычищен, длина — по потолку', () => {
    const transition = SagaTransitions.failure(
      definition,
      instance({}),
      { error: `a\u0000b${'x'.repeat(5_000)}`, retryable: true, uncertain: false, timedOut: false },
      NOW,
      () => 0.5,
    );
    const detail = transition.journal[0]?.detail ?? '';
    expect({ nul: detail.includes('\u0000'), long: detail.length <= 2_000 }).toEqual({
      nul: false,
      long: true,
    });
  });

  it('неизвестный исход шага без компенсации — запись uncompensated-uncertain; с компенсацией — нет', () => {
    const bare = defineSaga<{ id: string }>('u', { version: 1 })
      .step('a', { execute: noop, compensate: noop })
      .remoteStep('r', {
        command: () => ({ eventType: 'x', payload: {} }),
        timeoutMs: 1_000,
        retry: { maxAttempts: 1 },
      })
      .build();
    const at = (def: typeof bare, index: number) =>
      SagaTransitions.failure(
        def,
        instance({ name: def.name, steps: def.steps.map((step) => step.name), stepIndex: index }),
        { error: 'нет ответа', retryable: true, uncertain: true, timedOut: true },
        NOW,
        () => 0.5,
      ).journal.map((entry) => entry.event);

    expect(at(bare, 1)).toEqual(['step-timed-out', 'uncompensated-uncertain']);
    // Шаг d в общем определении — удалённый С компенсацией: отметки нет.
    expect(at(definition, 3)).not.toContain('uncompensated-uncertain');
  });

  it('compensationFrom: ближайший шаг с компенсацией не выше from; нет — -1', () => {
    expect([4, 3, 2, 1, 0, -1].map((from) => SagaTransitions.compensationFrom(definition, from))).toEqual([
      3, 3, 2, 0, 0, -1,
    ]);
  });

  describe('cancel', () => {
    it.each([
      // [шаг отмены, компенсации по порядку]: локальный текущий шаг не выполнен —
      // его компенсация не идёт; удалённый (d) — исход неизвестен, компенсируется.
      [1, ['a']],
      [2, ['a']],
      [3, ['d', 'c', 'a']],
      [4, ['d', 'c', 'a']],
    ])('на шаге %i: компенсации выполненных (и текущего удалённого) — в обратном порядке', (index, order) => {
      const cancelled = SagaTransitions.cancel(definition, instance({ stepIndex: index }), 'клиент', NOW);
      expect(compensationOrder({ ...instance({ stepIndex: index }), ...cancelled.state })).toEqual(order);
      expect({ generation: cancelled.state.generation, signals: cancelled.signals }).toEqual({
        generation: 1,
        signals: [],
      });
    });

    it('локальный шаг с компенсацией (c) не выполнен — первая компенсация ниже него (a), а не он сам', () => {
      const cancelled = SagaTransitions.cancel(definition, instance({ stepIndex: 2 }), 'клиент', NOW);
      expect(cancelled.command).toEqual({
        phase: 'compensate',
        stepIndex: 0,
        attempt: 1,
        generation: 1,
        delayMs: null,
      });
      // Двойник: текущий УДАЛЁННЫЙ шаг (d) компенсируется сам — команда могла уйти.
      const atD = SagaTransitions.cancel(definition, instance({ stepIndex: 3 }), 'клиент', NOW);
      expect(atD.command?.stepIndex).toBe(3);
    });

    it('удалённый шаг БЕЗ компенсации: запись uncompensated-uncertain, как при таймауте; с компенсацией — нет', () => {
      const bare = defineSaga<{ id: string }>('u', { version: 1 })
        .step('a', { execute: noop, compensate: noop })
        .remoteStep('r', { command: () => ({ eventType: 'x', payload: {} }), timeoutMs: 1_000 })
        .build();
      const events = (def: typeof bare, index: number) =>
        SagaTransitions.cancel(
          def,
          instance({
            name: def.name,
            steps: def.steps.map((step) => step.name),
            stepIndex: index,
            attempt: 2,
          }),
          'клиент',
          NOW,
        ).journal.map((entry) => `${entry.event}:${entry.step ?? ''}:${entry.generation}`);
      expect(events(bare, 1)).toEqual(['saga-cancelled::1', 'uncompensated-uncertain:r:0']);
      expect(events(definition, 3)).toEqual(['saga-cancelled::1']);
      // Двойник: локальный шаг без компенсации (b) не выполнен — неизвестности нет.
      expect(events(definition, 1)).toEqual(['saga-cancelled::1']);
    });

    it('компенсировать нечего: compensated сразу, без сигнала, но с командой-уведомлением на свой адрес', () => {
      const cancelled = SagaTransitions.cancel(
        definition,
        instance({ stepIndex: 0, attempt: 2, generation: 3 }),
        'клиент',
        NOW,
      );
      expect({
        status: cancelled.state.status,
        signals: cancelled.signals,
        command: cancelled.command,
      }).toEqual({
        status: SAGA_STATUS.compensated,
        signals: [],
        command: { phase: 'compensate', stepIndex: 0, attempt: 2, generation: 4, delayMs: null },
      });
    });
  });
});
