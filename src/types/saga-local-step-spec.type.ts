import type { SagaContextMerge } from './saga-context-merge.type';
import type { SagaRetryOptions } from './saga-retry-options.type';
import type { SagaStepArgs } from './saga-step-args.type';
import type { SagaStepOutput } from './saga-step-output.type';

/**
 * Локальный шаг: выполняется исполнителем саги в транзакции, где пишутся
 * состояние экземпляра, журнал и следующая команда. Бросил — транзакция
 * откатилась целиком (эффекты шага тоже), отказ записывается отдельно:
 * повтор с паузой или компенсации. `SagaStepRejectedError` — без повторов.
 */
// biome-ignore lint/suspicious/noConfusingVoidType: шаг без выхода — `async () => {}`, то есть Promise<void>
export interface SagaLocalStepSpec<Ctx, Out extends object | void> {
  execute(args: SagaStepArgs<Ctx>): Promise<Out>;
  /**
   * Отмена завершённого шага; нет — шаг при компенсациях пропускается.
   * Контекст — после шага (поля выхода перекрывают прежние, как в рантайме);
   * поля, которые позже перезаписали следующие шаги (уже компенсированные),
   * приходят в их значении — на месте шага тип их не знает.
   */
  compensate?(args: SagaStepArgs<SagaContextMerge<Ctx, SagaStepOutput<Out>>>): Promise<void>;
  readonly retry?: SagaRetryOptions;
  readonly compensationRetry?: SagaRetryOptions;
  /** Потолок выполнения шага (и компенсации), мс: по нему — отказ с повтором. */
  readonly timeoutMs?: number;
}
