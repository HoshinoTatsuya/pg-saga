/** Типы событий outbox, которыми сага двигает сама себя (`SAGA_EVENT_TYPE`). */
export interface SagaEventTypes {
  readonly command: string;
  readonly timer: string;
  readonly reply: string;
}
