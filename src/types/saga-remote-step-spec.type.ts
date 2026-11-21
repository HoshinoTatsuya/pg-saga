import type { SagaRemoteArgs } from './saga-remote-args.type';
import type { SagaRemoteCommand } from './saga-remote-command.type';
import type { SagaRetryOptions } from './saga-retry-options.type';
import type { SagaUncertainMerge } from './saga-uncertain-merge.type';

/**
 * Удалённый шаг: команда участнику через outbox, ответ — `handleReply` (или
 * событие ответа в том же outbox). Исход по таймауту неизвестен: участник мог
 * выполнить шаг, а ответ не дошёл. Поэтому при компенсациях после таймаута
 * компенсируется и сам этот шаг — компенсация участника обязана переносить
 * «отменять нечего», а её контекст — `SagaUncertainMerge`: поля выхода шага
 * могли не прийти (новые — необязательны, перекрытые — прежнего ИЛИ нового типа).
 *
 * Компенсация получает ТЕКУЩИЙ контекст экземпляра: поля, которые позже
 * перезаписали следующие шаги (сами уже компенсированные), приходят в их
 * значении. Тип этого не видит — на месте шага поздние шаги ещё не известны.
 */
export interface SagaRemoteStepSpec<Ctx, Out extends object> {
  command(args: SagaRemoteArgs<Ctx>): SagaRemoteCommand;
  compensation?(args: SagaRemoteArgs<SagaUncertainMerge<Ctx, Out>>): SagaRemoteCommand;
  /** Таймер ответа (событие с `deliverAt`), мс. Обязателен: без него шаг ждал бы ответа вечно. */
  readonly timeoutMs: number;
  readonly retry?: SagaRetryOptions;
  readonly compensationRetry?: SagaRetryOptions;
}
