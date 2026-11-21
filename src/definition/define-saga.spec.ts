import { SAGA_DEFAULTS } from '../constants/saga-defaults.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { defineSaga } from './define-saga';

const noop = async (): Promise<void> => undefined;

describe('defineSaga', () => {
  it('шаги по порядку, повтор по умолчанию и свой, таймауты; построитель неизменяем', () => {
    const base = defineSaga<{ id: string }>('order', { version: 3 }).step('a', { execute: noop });
    const one = base.step('b', { execute: noop, retry: { maxAttempts: 7 }, timeoutMs: 50 }).build();
    const other = base
      .remoteStep('c', { command: () => ({ eventType: 'x', payload: 1 }), timeoutMs: 10 })
      .build();

    expect(one.steps.map((step) => [step.name, step.kind, step.timeoutMs, step.retry.maxAttempts])).toEqual([
      // Без своего потолка — потолок по умолчанию (меньше доставки релея), не «без потолка».
      ['a', 'local', SAGA_DEFAULTS.localStepTimeoutMs, SAGA_DEFAULTS.stepRetry.maxAttempts],
      ['b', 'local', 50, 7],
    ]);
    expect(other.steps.map((step) => step.name)).toEqual(['a', 'c']);
    expect(one.steps[0]?.compensationRetry).toEqual(SAGA_DEFAULTS.compensationRetry);
    expect({ name: one.name, version: one.version, frozen: Object.isFrozen(one.steps) }).toEqual({
      name: 'order',
      version: 3,
      frozen: true,
    });
  });

  it('шаг меняет тип поля контекста — следующий шаг видит НОВЫЙ тип (не never)', async () => {
    // Проверка — компиляцией: при пересечении `Ctx & Out` контекст здесь
    // схлопывался в never, и `context.status` не компилировалось.
    const seen: string[] = [];
    const saga = defineSaga<{ status: 'new'; amount: number | string }>('typed', { version: 1 })
      .step('pay', { execute: async () => ({ status: 'paid' as const, amount: 5 }) })
      .step('ship', {
        execute: async ({ context }) => {
          const status: 'paid' = context.status;
          const amount: number = context.amount;
          seen.push(`${status}:${amount}`);
        },
      })
      .build();
    expect(saga.steps.map((step) => step.name)).toEqual(['pay', 'ship']);
    await saga.steps[1]?.execute?.({ context: { status: 'paid', amount: 5 } } as never);
    expect(seen).toEqual(['paid:5']);
  });

  it.each([
    ['пустое имя саги', () => defineSaga('', { version: 1 })],
    ['версия 0', () => defineSaga('s', { version: 0 })],
    ['версия дробная', () => defineSaga('s', { version: 1.5 })],
    ['нет шагов', () => defineSaga('s', { version: 1 }).build()],
    [
      'повтор имени шага',
      () => defineSaga('s', { version: 1 }).step('a', { execute: noop }).step('a', { execute: noop }),
    ],
    ['нет execute', () => defineSaga('s', { version: 1 }).step('a', {} as never)],
    [
      'remoteStep без timeoutMs',
      () =>
        defineSaga('s', { version: 1 }).remoteStep('a', {
          command: () => ({ eventType: 'x', payload: 1 }),
        } as never),
    ],
    [
      'maxAttempts 0',
      () => defineSaga('s', { version: 1 }).step('a', { execute: noop, retry: { maxAttempts: 0 } }),
    ],
    [
      'крышка паузы меньше базы',
      () =>
        defineSaga('s', { version: 1 }).step('a', {
          execute: noop,
          retry: { backoffMs: 10, maxBackoffMs: 5 },
        }),
    ],
    ['имя шага длиннее 64', () => defineSaga('s', { version: 1 }).step('x'.repeat(65), { execute: noop })],
    // Имена уходят в jsonb (steps) и text: непарный суррогат jsonb отвергает (22P02
    // на start в транзакции вызывающего), а text драйвер заменяет на U+FFFD.
    [
      'непарный суррогат в имени шага',
      () => defineSaga('s', { version: 1 }).step('a\ud800', { execute: noop }),
    ],
    ['непарный суррогат в имени саги', () => defineSaga('s\udc00', { version: 1 })],
  ])('%s — отказ при определении', (_label, build) => {
    expect(build).toThrow(SagaConfigError);
  });

  it('типы: следующий шаг видит выход предыдущих, чужое поле не компилируется', () => {
    const saga = defineSaga<{ orderId: string }>('typed', { version: 1 })
      .step('reserve', { execute: async () => ({ reservationId: 'r' }) })
      .remoteStep<{ chargeId: string }>('charge', {
        command: ({ context }) => ({ eventType: 'charge', payload: context.reservationId }),
        compensation: ({ context }) => ({ eventType: 'refund', payload: context.chargeId ?? null }),
        timeoutMs: 1,
      })
      .step('ship', {
        execute: async ({ context }) => {
          const ids: string[] = [context.orderId, context.reservationId, context.chargeId];
          // @ts-expect-error — поля, которого не даёт ни один шаг, в контексте нет
          const missing: string = context.trackingId;
          return { shipped: ids.join(':') + String(missing) };
        },
        compensate: async ({ context }) => {
          const shipped: string = context.shipped;
          expect(shipped).toBeDefined();
        },
      })
      .build();

    expect(saga.steps.map((step) => step.name)).toEqual(['reserve', 'charge', 'ship']);
  });
});
