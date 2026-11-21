import type { SagaContext } from './saga-context.type';
import type { SagaStatus } from './saga-status.type';

/** Строка `saga_instances`. */
export interface SagaInstance {
  readonly id: string;
  readonly name: string;
  readonly version: number;
  /** Имена шагов определения, с которым экземпляр начат. */
  readonly steps: readonly string[];
  readonly status: SagaStatus;
  /** Шаг, который выполняется (`running`) или компенсируется (`compensating`, `stuck`). */
  readonly stepIndex: number;
  readonly attempt: number;
  /**
   * Воплощение — случайное целое, выбранное при старте: входит в id сообщений
   * экземпляра и в ключ шага (`sagaStepKey`, формат 2). id саги можно занять
   * снова после `prune`, и даже при том же моменте старта (неподвижные часы
   * теста, `now` старта из времени события) сообщения и ключи нового
   * экземпляра не совпадут с прежними. `null` — экземпляр начат версией 0.1.0:
   * он доживает с её форматом id и ключа (`<sagaId>:<фаза>:<номер>`).
   */
  readonly incarnation: number | null;
  /** Растёт при ручном повторе: сообщения прошлого поколения не применяются. */
  readonly generation: number;
  readonly context: SagaContext;
  /** Отказ, с которого начались компенсации. */
  readonly failure: string | null;
  readonly lastError: string | null;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly finishedAt: Date | null;
  /**
   * Когда экземпляр ждёт следующего сообщения: срок отложенной команды (пауза
   * повтора) или таймера ответа удалённого шага. Пока он не наступил,
   * неподвижный экземпляр не «замёрз», а ждёт.
   */
  readonly wakeAt: Date | null;
}
