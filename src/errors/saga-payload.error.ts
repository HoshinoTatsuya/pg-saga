/** Сообщение саги (команда, таймер, ответ) или выход шага — не той формы. */
export class SagaPayloadError extends Error {
  public override readonly name = 'SagaPayloadError';

  public constructor(detail: string) {
    super(`pg-saga: ${detail}`);
  }
}
