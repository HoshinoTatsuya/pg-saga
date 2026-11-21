/** Куда оркестратор пишет предупреждения (отказ хука метрик и т. п.). */
export interface SagaLogger {
  warn(message: string): void;
}
