/** Бюджет локального шага: момент потолка (`performance.now()`, мс) и сам потолок. */
export interface SagaStepBudget {
  readonly deadline: number;
  readonly limitMs: number;
}
