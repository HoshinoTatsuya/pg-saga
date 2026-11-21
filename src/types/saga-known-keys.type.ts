/**
 * Явно объявленные ключи типа — без индексных сигнатур (`Record<string, X>`
 * даёт `keyof` = `string`). Слияние контекста по `keyof` словаря стирало бы все
 * прежние поля: `Omit<Ctx, string>` — пустой объект.
 */
export type SagaKnownKeys<T> = keyof {
  [K in keyof T as string extends K ? never : number extends K ? never : symbol extends K ? never : K]: T[K];
};
