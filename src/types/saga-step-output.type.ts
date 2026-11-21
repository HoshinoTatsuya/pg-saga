/**
 * Что шаг добавляет к контексту: объект выхода (тип-литерал, интерфейс,
 * экземпляр класса — его собственные поля после JSON) или ничего (`void`).
 */
export type SagaStepOutput<Out> = Out extends object ? Out : Record<never, never>;
