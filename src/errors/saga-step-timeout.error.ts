/** Локальный шаг не уложился в `timeoutMs`: транзакция шага откатывается, отказ — с повтором. */
export class SagaStepTimeoutError extends Error {
  public override readonly name = 'SagaStepTimeoutError';

  public constructor(public readonly timeoutMs: number) {
    super(`шаг не уложился в ${timeoutMs} мс`);
  }
}
