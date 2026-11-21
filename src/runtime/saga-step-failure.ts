/**
 * Отказ САМОГО шага (его функции, построителя команды, формы выхода), а не
 * инфраструктуры. Только он записывается как отказ шага (повтор или
 * компенсации); обрыв соединения, неизвестный исход COMMIT и прочие отказы
 * транзакции уходят наружу — доставку повторит outbox, а inbox не даст
 * применить её дважды.
 */
export class SagaStepFailure extends Error {
  public override readonly name = 'SagaStepFailure';

  public constructor(public readonly reason: unknown) {
    super('отказ шага саги', { cause: reason });
  }
}
