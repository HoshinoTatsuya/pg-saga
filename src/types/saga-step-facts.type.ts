/**
 * Что журнал экземпляра знает о шаге, на который пришёл НЕОЖИДАННЫЙ ответ:
 * уходила ли команда ровно этой попытки и поколения (`dispatched`) и выполнен
 * ли уже шаг в этой фазе какой-либо попыткой (`done`: `step-completed` для
 * выполнения, `step-compensated` для компенсации).
 */
export interface SagaStepFacts {
  readonly dispatched: boolean;
  readonly done: boolean;
}
