import type { SagaKnownKeys } from './saga-known-keys.type';

/**
 * Контекст после шага, чей исход НЕИЗВЕСТЕН (компенсация удалённого шага после
 * таймаута): выход мог прийти, а мог нет. Поля только из выхода —
 * необязательны; поля, которые выход перекрывает, — прежнего ИЛИ нового типа;
 * прочие — как были. Пересечение `Ctx & Partial<Out>` схлопывало в `never`
 * поле, которому шаг меняет тип (`status: 'new'` → `'charged'`). Дистрибутивно
 * по вариантам, как `SagaContextMerge`; поля словаря (индексная сигнатура
 * выхода) не добавляются — выход мог не прийти.
 */
export type SagaUncertainMerge<Ctx, Out> = Ctx extends unknown
  ? Out extends unknown
    ? Omit<Ctx, SagaKnownKeys<Out>> & {
        [K in Exclude<SagaKnownKeys<Out>, keyof Ctx>]?: Out[K];
      } & {
        [K in Extract<SagaKnownKeys<Out>, keyof Ctx>]: Ctx[K] | Out[K];
      }
    : never
  : never;
