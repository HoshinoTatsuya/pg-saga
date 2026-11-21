import type { SagaInstance } from '../types/saga-instance.type';

/**
 * Время перехода: часы `now`, снятые ПОСЛЕ блокировки строки экземпляра, и
 * не раньше его последнего сдвига. Снятое до ожидания строки (шаг, державший
 * её, шёл секундами) или на реплике с отстающими часами, оно уводило бы
 * `updated_at` и журнал назад — и отчёт о замёрзших, и срез считали бы простой
 * от момента, которого не было.
 */
export const sagaTimeAfter = (now: Date, instance: Pick<SagaInstance, 'updatedAt'>): Date =>
  now.getTime() < instance.updatedAt.getTime() ? new Date(instance.updatedAt.getTime()) : now;
