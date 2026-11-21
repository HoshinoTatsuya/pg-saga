import type { OutboxTransaction } from '@hoshinotatsuya/outbox';

/** Что получает локальный шаг (`execute`) и его компенсация (`compensate`). */
export interface SagaStepArgs<Ctx> {
  /**
   * Транзакция шага: в ней же пишутся состояние экземпляра, журнал и
   * следующая команда. Запись шага через неё атомарна с продвижением саги;
   * запись мимо неё (другое соединение, HTTP) — at least once.
   */
  readonly tx: OutboxTransaction;
  readonly context: Readonly<Ctx>;
  readonly sagaId: string;
  /**
   * Ключ идемпотентности шага `<sagaId>#<воплощение>:<фаза>:<номер шага>` —
   * один во всех попытках и поколениях этого экземпляра, другой у нового
   * экземпляра с тем же id: его стоит передавать внешней системе
   * (`Idempotency-Key`).
   */
  readonly stepKey: string;
  /**
   * Ключ ВЫПОЛНЕНИЯ этого шага (в фазе выполнения равен `stepKey`): компенсация
   * по нему находит то, что отменять, даже если исход выполнения неизвестен и
   * в контекст ничего не попало.
   */
  readonly executeStepKey: string;
  /** Номер попытки, с 1. */
  readonly attempt: number;
  /** Прерывается по `timeoutMs` шага и по потолку доставки релея. */
  readonly signal: AbortSignal;
}
