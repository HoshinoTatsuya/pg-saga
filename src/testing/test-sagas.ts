import { defineSaga } from '../definition/define-saga';
import { SagaStepRejectedError } from '../errors/saga-step-rejected.error';
import type { SagaStepArgs } from '../types/saga-step-args.type';
import { recordEffect } from './saga-rig';

/**
 * Сценарий отказов: сколько раз отказать (`n` — первые n попыток, `always`),
 * `reject` — окончательный отказ, `hang` — висеть до сигнала. Ключ — имя шага.
 */
export interface FailureScript {
  readonly execute: Map<string, number | 'always' | 'reject' | 'hang'>;
  readonly compensate: Map<string, number | 'always' | 'reject'>;
  /** Вызов внутри шага ДО его эффекта (гонки, обрыв соединения). */
  inside?: ((step: string, args: SagaStepArgs<unknown>) => Promise<void>) | undefined;
}

export const newScript = (): FailureScript => ({ execute: new Map(), compensate: new Map() });

const shouldFail = (rule: number | 'always' | 'reject' | 'hang' | undefined, attempt: number): boolean =>
  rule === 'always' || (typeof rule === 'number' && attempt <= rule);

const runScripted = async (
  script: FailureScript,
  step: string,
  phase: 'execute' | 'compensate',
  args: SagaStepArgs<unknown>,
): Promise<void> => {
  await script.inside?.(step, args);
  const rule = script[phase].get(step);
  if (rule === 'reject') {
    throw new SagaStepRejectedError(`${step}: окончательный отказ`);
  }
  if (rule === 'hang') {
    await new Promise((_resolve, reject) => {
      args.signal.addEventListener('abort', () => reject(args.signal.reason), { once: true });
    });
  }
  // Эффект пишется ДО отказа: проверка видит, что откат шага снял и его.
  await recordEffect(args.tx, args.sagaId, step, phase, args.attempt);
  if (shouldFail(rule, args.attempt)) {
    throw new Error(`${step}: отказ попытки ${args.attempt}`);
  }
};

/**
 * Сага из четырёх локальных шагов: `reserve` (компенсация), `notify` (БЕЗ
 * компенсации — при откате пропускается), `charge` (компенсация), `ship`
 * (компенсация). Каждый шаг пишет эффект в своей транзакции и отдаёт поле в
 * контекст; следующий шаг проверяет, что видит поля предыдущих.
 */
export const orderSaga = (script: FailureScript, version = 1, timeoutMs?: number) =>
  defineSaga<{ orderId: string }>('order', { version })
    .step('reserve', {
      execute: async (args) => {
        await runScripted(script, 'reserve', 'execute', args);
        return { reservationId: `r-${args.context.orderId}` };
      },
      compensate: (args) => runScripted(script, 'reserve', 'compensate', args),
      retry: { maxAttempts: 3, backoffMs: 1_000 },
      compensationRetry: { maxAttempts: 3, backoffMs: 1_000 },
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    })
    .step('notify', {
      execute: async (args) => {
        await runScripted(script, 'notify', 'execute', args);
      },
      retry: { maxAttempts: 3, backoffMs: 1_000 },
    })
    .step('charge', {
      execute: async (args) => {
        if (args.context.reservationId !== `r-${args.context.orderId}`) {
          throw new SagaStepRejectedError('контекст не дошёл до шага charge');
        }
        await runScripted(script, 'charge', 'execute', args);
        return { chargeId: `c-${args.context.reservationId}` };
      },
      compensate: (args) => runScripted(script, 'charge', 'compensate', args),
      retry: { maxAttempts: 3, backoffMs: 1_000 },
      compensationRetry: { maxAttempts: 3, backoffMs: 1_000 },
    })
    .step('ship', {
      execute: async (args) => {
        await runScripted(script, 'ship', 'execute', args);
        return { shipped: args.context.chargeId };
      },
      compensate: (args) => runScripted(script, 'ship', 'compensate', args),
      retry: { maxAttempts: 3, backoffMs: 1_000 },
      compensationRetry: { maxAttempts: 3, backoffMs: 1_000 },
    })
    .build();
