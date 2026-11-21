import type { SagaRunnerLimits } from '../types/saga-runner-limits.type';
import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';

/**
 * Потолки раннеров, построенных `sagaTransactionRunner`: `register` сверяет
 * локальные шаги с тем, что раннер РЕАЛЬНО применяет (ожидание строки,
 * ROLLBACK, простой транзакции). Своя обёртка (`deps.transaction`) сюда не
 * попадает — её потолки берутся из настроек оркестратора.
 */
export const sagaRunnerLimits = new WeakMap<SagaTransactionRunner, SagaRunnerLimits>();
