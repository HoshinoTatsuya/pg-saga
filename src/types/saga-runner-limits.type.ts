/**
 * Потолки, которые раннер `sagaTransactionRunner` применяет к своей
 * транзакции, мс. По ним `register` считает бюджет доставки локального шага:
 * число из настроек оркестратора расходилось бы с тем, что применяет раннер.
 */
export interface SagaRunnerLimits {
  /** `idle_in_transaction_session_timeout`: шаг между своими запросами — простой. */
  readonly idleInTransactionTimeoutMs: number;
  /** `lock_timeout`: ожидание строки экземпляра и прочих блокировок. */
  readonly lockTimeoutMs: number;
  /** Потолок ROLLBACK после отказа шага (дольше — соединение уничтожается). */
  readonly rollbackTimeoutMs: number;
}
