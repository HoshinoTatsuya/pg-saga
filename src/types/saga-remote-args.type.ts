/** Что получает построитель команды удалённого шага. */
export interface SagaRemoteArgs<Ctx> {
  readonly context: Readonly<Ctx>;
  readonly sagaId: string;
  /** Ключ идемпотентности шага у участника — один во всех попытках (см. `SagaStepArgs.stepKey`). */
  readonly stepKey: string;
  /**
   * Ключ ВЫПОЛНЕНИЯ шага (в фазе выполнения равен `stepKey`): построитель
   * компенсации передаёт его участнику, чтобы отмена нашла операцию, чей исход
   * оркестратору неизвестен (таймаут ответа).
   */
  readonly executeStepKey: string;
  readonly attempt: number;
}
