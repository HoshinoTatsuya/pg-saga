/** Итог прохода сторожа (`SagaStallWatch.runOnce`). */
export interface SagaStallWatchReport {
  /** Сколько неподвижных активных проверено на DEAD ожидаемого сообщения. */
  readonly checked: number;
  /** id экземпляров, переведённых в `blocked` этим проходом. */
  readonly blocked: readonly string[];
  /** id экземпляров с НОВЫМ эпизодом `stalled` (хук вызван этим проходом). */
  readonly stalled: readonly string[];
}
