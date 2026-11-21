import { integer } from '../config/config-integer';
import { sagaName } from '../config/config-name';
import { SagaBuilder } from './saga-builder';

/**
 * Начать определение саги. `Input` — вход (начальный контекст), `version` —
 * версия определения: меняйте её при любой правке состава или порядка шагов,
 * иначе летящие экземпляры получат отказ «определение разошлось» (это лучше,
 * чем доехать по чужим шагам).
 *
 * ```ts
 * const order = defineSaga<{ orderId: string }>('order', { version: 1 })
 *   .step('reserve', { execute: async ({ tx, context }) => ({ reservationId: … }), compensate: … })
 *   .remoteStep<{ chargeId: string }>('charge', { command: ({ context }) => …, timeoutMs: 60_000 })
 *   .build();
 * ```
 */
export const defineSaga = <Input extends object>(
  name: string,
  options: { readonly version: number },
): SagaBuilder<Input, Input> =>
  new SagaBuilder<Input, Input>(sagaName('saga', name), integer('version', options?.version, 1), []);
