import { SAGA_STEP_REJECTED_MARK as REJECTED } from './saga-step-rejected-mark.constant';

/**
 * Окончательный отказ шага: бросьте из `execute`, чтобы не тратить повторы
 * (бизнес-отказ: «нет на складе», «карта отклонена») — компенсации начнутся
 * сразу. Из `compensate` — экземпляр сразу уходит в `stuck`.
 *
 * Распознаётся по метке, а не `instanceof`: шаг может бросить его из другой
 * копии пакета (библиотека участника на своей версии), и `instanceof` тогда
 * ложен — окончательный отказ тратил бы повторы как обычный.
 */
export class SagaStepRejectedError extends Error {
  public override readonly name = 'SagaStepRejectedError';

  public readonly [REJECTED] = true;

  /**
   * Окончательный отказ из ЛЮБОЙ копии пакета — и обёрнутый: транспорт или
   * код шага часто оборачивают отказ (`new Error('…', { cause })`), и без
   * обхода `cause` окончательный отказ тратил бы повторы.
   */
  public static is(error: unknown): boolean {
    let current: unknown = error;
    for (let depth = 0; depth < 5 && typeof current === 'object' && current !== null; depth += 1) {
      if ((current as { [REJECTED]?: unknown })[REJECTED] === true) {
        return true;
      }
      current = (current as { cause?: unknown }).cause;
    }
    return false;
  }
}
