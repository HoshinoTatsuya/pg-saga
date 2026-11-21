/** Срез по одной саге (`SagaAdmin.stats`). */
export interface SagaStats {
  readonly saga: string;
  readonly running: number;
  readonly compensating: number;
  readonly stuck: number;
  /** Встали на DEAD ожидаемого сообщения (`SagaStallWatch`) и ждут человека. */
  readonly blocked: number;
  /** Начато с момента `since`. */
  readonly started: number;
  /** Завершено успешно с `since`. */
  readonly completed: number;
  /** Завершено компенсациями с `since`. */
  readonly compensated: number;
  /** Разобраны вручную (`resolve`) с `since`: иначе они пропадали из среза после `stuck`. */
  readonly resolved: number;
  /** Возраст старейшего активного (`running`/`compensating`) экземпляра; нет активных — `null`. */
  readonly oldestActiveAgeMs: number | null;
  /**
   * Дольше всех не двигавшийся running/compensating (по `updated_at`): сигнал
   * «сага замёрзла» — доставка ушла в DEAD, а экземпляр об этом не знает.
   * `oldestActiveAgeMs` (по `created_at`) долгую сагу от замёрзшей не отличает.
   */
  readonly longestIdleActiveMs: number | null;
}
