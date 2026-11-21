/** Отказ шага в терминах перехода. */
export interface SagaFailure {
  readonly error: string;
  /** `false` — без повторов (`SagaStepRejectedError`, `retryable: false` в ответе). */
  readonly retryable: boolean;
  /**
   * Исход шага неизвестен (таймаут удалённого шага: участник мог выполнить
   * его, а ответ не дошёл) — при компенсациях компенсируется и он сам.
   */
  readonly uncertain: boolean;
  readonly timedOut: boolean;
}
