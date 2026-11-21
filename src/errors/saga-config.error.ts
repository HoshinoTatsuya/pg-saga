/** Неверное определение саги, настройка или аргумент — до обращения к базе. */
export class SagaConfigError extends Error {
  public override readonly name = 'SagaConfigError';

  public constructor(
    public readonly field: string,
    detail: string,
  ) {
    super(`pg-saga: ${field} — ${detail}`);
  }
}
