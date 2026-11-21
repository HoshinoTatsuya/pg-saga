import type { SagaKnownKeys } from './saga-known-keys.type';

/**
 * Контекст после шага: поля выхода шага ПЕРЕКРЫВАЮТ одноимённые прежние — так
 * же, как `{ ...context, ...output }` в рантайме. Пересечение `Ctx & Out`
 * схлопывало контекст в `never`, если шаг менял тип поля (`status: 'new'` →
 * `'paid'`), и следующий шаг не компилировался.
 *
 * Дистрибутивно по вариантам входа и выхода: `Omit` над объединением оставляет
 * только общие поля, и вход-объединение (`{ kind: 'retail'; sku } | { kind:
 * 'b2b'; contractId }`) терял бы поля вариантов после первого же шага.
 * Перекрываются только ЯВНЫЕ ключи выхода: выход-словарь (`Record<string, X>`)
 * прежних полей в типе не стирает — их значения в рантайме на месте, пока
 * словарь не содержит того же ключа.
 */
export type SagaContextMerge<Ctx, Out> = Ctx extends unknown
  ? Out extends unknown
    ? Omit<Ctx, SagaKnownKeys<Out>> & Out
    : never
  : never;
