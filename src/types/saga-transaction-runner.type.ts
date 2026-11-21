import type { OutboxTransaction } from '@hoshinotatsuya/outbox';

/**
 * Как открыть свою транзакцию. По умолчанию — `transactionKeepingFailure`
 * из `@hoshinotatsuya/pg-runtime` над базой оркестратора (`sagaTransactionRunner`).
 */
export type SagaTransactionRunner = <T>(work: (tx: OutboxTransaction) => Promise<T>) => Promise<T>;
