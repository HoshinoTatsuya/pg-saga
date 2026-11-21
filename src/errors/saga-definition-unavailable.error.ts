/**
 * Определения, по которому начат экземпляр, в этом процессе нет: версия не
 * зарегистрирована (выкатка: экземпляр новой версии пришёл на старую реплику)
 * или зарегистрированная версия разошлась с записанной в экземпляре (шаги
 * переименованы или переставлены без смены версии). Экземпляр не трогается,
 * доставка отказывает — outbox повторит её на реплике с нужной версией.
 */
export class SagaDefinitionUnavailableError extends Error {
  public override readonly name = 'SagaDefinitionUnavailableError';

  public constructor(
    public readonly saga: string,
    public readonly version: number,
    public readonly reason: string,
  ) {
    super(`pg-saga: сага ${saga} версии ${version}: ${reason}`);
  }
}
