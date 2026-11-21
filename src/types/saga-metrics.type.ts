import type { SagaPhase } from './saga-phase.type';
import type { SagaStatus } from './saga-status.type';

/**
 * Хук метрик: переходы экземпляров, вызывается ПОСЛЕ фиксации транзакции
 * (откатившийся переход не считается). Отказ хука проглатывается с
 * предупреждением: наблюдение не вправе ломать сагу. Срезы «сколько сейчас»
 * (активные, застрявшие, возраст старейшего, запущено за окно) — `SagaAdmin.stats`.
 */
export interface SagaMetrics {
  completed?(event: { readonly saga: string; readonly sagaId: string; readonly durationMs: number }): void;
  /** Экземпляр компенсирован — один раз, в том числе после отмены, которой нечего было компенсировать. */
  compensated?(event: {
    readonly saga: string;
    readonly sagaId: string;
    readonly failure: string | null;
  }): void;
  stuck?(event: { readonly saga: string; readonly sagaId: string; readonly error: string | null }): void;
  stepFailed?(event: {
    readonly saga: string;
    readonly sagaId: string;
    readonly step: string;
    readonly phase: SagaPhase;
    readonly attempt: number;
    readonly willRetry: boolean;
    readonly error: string;
  }): void;
  /**
   * Шаг или его компенсация выполнены. `sinceLastMoveMs` — от предыдущего сдвига
   * экземпляра до завершения: для локального шага — ожидание доставки и сам
   * шаг, для удалённого — от отправки команды до ответа.
   */
  stepCompleted?(event: {
    readonly saga: string;
    readonly sagaId: string;
    readonly step: string;
    readonly phase: SagaPhase;
    readonly attempt: number;
    readonly sinceLastMoveMs: number;
  }): void;
  /**
   * Сообщение экземпляра, чьей версии определения в процессе нет (или она
   * разошлась с записанной). Отказ доставки из-за определения: ОДИН вызов на каждую отказанную доставку
   * (повторы релея — тоже), то есть это счётчик отказов, а не событие «случилось».
   */
  /**
   * Экземпляр встал (`blocked`): доставка сообщения, которого он ждал, ушла в
   * DEAD outbox (`SagaStallWatch`, после фиксации). Один раз на экземпляр и
   * адрес: следующий вызов — только после ручного повтора и нового DEAD.
   */
  blocked?(event: {
    readonly saga: string;
    readonly sagaId: string;
    readonly step: string;
    readonly phase: SagaPhase;
    readonly attempt: number;
    /** id сообщения outbox в DEAD и его тип. */
    readonly messageId: string;
    readonly eventType: string;
    readonly error: string | null;
  }): void;
  /**
   * Активный экземпляр не двигается дольше порога `SagaStallWatch`
   * (`stalledAfterMs`), а DEAD его сообщения нет: бэклог релея, долгий шаг,
   * сообщение потеряно. Один вызов на эпизод (экземпляр, адрес, момент
   * последнего движения) в процессе; повтора экземпляра сторож не делает.
   */
  stalled?(event: {
    readonly saga: string;
    readonly sagaId: string;
    readonly status: SagaStatus;
    readonly step: string;
    readonly attempt: number;
    readonly stalledForMs: number;
  }): void;
  definitionUnavailable?(event: {
    readonly saga: string;
    readonly version: number;
    readonly reason: string;
  }): void;
}
