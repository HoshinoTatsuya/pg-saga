import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';

/**
 * Выражения CHECK таблиц саги — одни для DDL (`sagaTablesSql`), схемы drizzle
 * (`defineSagaTables`) и доводки существующих таблиц (`sagaUpgradeSql`).
 * Значения — из словарей пакета (`SAGA_STATUS`, `SAGA_PHASE`), а не списком
 * рядом: новый статус попадает в ограничение сам.
 */
export const SAGA_CHECK_SQL = {
  /** Экземпляр: известный статус, шаг с нуля, попытка с единицы, поколение с нуля. */
  instancesState: `"status" IN (${Object.values(SAGA_STATUS)
    .map((value) => `'${value}'`)
    .join(', ')}) AND "step_index" >= 0 AND "attempt" >= 1 AND "generation" >= 0`,
  /** Журнал: фаза — известная или нет её вовсе (старт, ручной разбор). */
  journalPhase: `"phase" IS NULL OR "phase" IN (${Object.values(SAGA_PHASE)
    .map((value) => `'${value}'`)
    .join(', ')})`,
} as const;
