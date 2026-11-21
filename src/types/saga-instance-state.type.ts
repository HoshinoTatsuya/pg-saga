import type { SagaInstance } from './saga-instance.type';

/** Изменяемая часть экземпляра: то, что переход пишет целиком. */
export type SagaInstanceState = Pick<
  SagaInstance,
  'status' | 'stepIndex' | 'attempt' | 'generation' | 'context' | 'failure' | 'lastError' | 'finishedAt'
>;
