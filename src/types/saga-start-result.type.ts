import type { SagaStatus } from './saga-status.type';

/** Итог `start`: `false` — экземпляр с этим id уже есть (повтор старта ничего не делает). */
export interface SagaStartResult {
  readonly started: boolean;
  /**
   * `started: false` — кем занят id: та же сага и версия (повтор старта) или
   * другая (id занят чужим процессом). Нет — экземпляра уже нет (гонка с prune).
   */
  readonly existing?: { readonly name: string; readonly version: number; readonly status: SagaStatus };
}
