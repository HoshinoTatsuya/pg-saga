import type { OutboxTransaction } from '@hoshinotatsuya/outbox';
import { type PoolDatabase, transactionKeepingFailure } from '@hoshinotatsuya/pg-runtime';

import { integer } from '../config/config-integer';
import { SAGA_DEFAULTS } from '../constants/saga-defaults.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';
import { sagaRunnerLimits } from './saga-runner-limits';

/**
 * Транзакция саги — `transactionKeepingFailure` из `@hoshinotatsuya/pg-runtime`:
 * соединение не утекает и не возвращается в пул сломанным, обрыв после COMMIT —
 * `CommitOutcomeUnknownError` (доставку повторит outbox, inbox решит, было ли),
 * повтор всей работы на 40001/40P01, потолок ожидания блокировок.
 *
 * И потолок ПРОСТОЯ транзакции (`idle_in_transaction_session_timeout`):
 * клиент, потерявший сеть посреди шага (замёрзший маршрут, пауза узла),
 * отпускает соединение по своим таймаутам, а сервер этого не видит — сессия
 * стоит «idle in transaction» с блокировкой строки экземпляра и ключом inbox,
 * пока не оттает сеть или не сработает TCP keepalive сервера (часы). Отмена и
 * доставки экземпляра всё это время отказывают по ожиданию блокировки.
 * Потолок обязан быть больше самого долгого простоя ЗАКОННОГО шага (шаг между
 * своими запросами ждёт внешний вызов): оркестратор берёт потолок доставки
 * релея (`deliveryTimeoutMs`), который больше любого локального шага.
 *
 * Потолки раннера (`lockTimeoutMs`, `rollbackTimeoutMs`, простой) он
 * регистрирует за собой: `register` оркестратора считает бюджет доставки по
 * ним, а не по своим настройкам, которые до этого раннера не доходят.
 */
export const sagaTransactionRunner = (
  db: unknown,
  options: {
    readonly lockTimeoutMs?: number;
    readonly attempts?: number;
    readonly idleInTransactionTimeoutMs?: number;
    /** Потолок ROLLBACK, мс; по умолчанию `SAGA_DEFAULTS.rollbackTimeoutMs`. */
    readonly rollbackTimeoutMs?: number;
  } = {},
): SagaTransactionRunner => {
  if (typeof (db as { $client?: unknown } | null)?.$client !== 'object') {
    throw new SagaConfigError(
      'db',
      'своя транзакция саги нужна над пулом pg (drizzle(pool)); иначе передайте deps.transaction',
    );
  }
  const lockTimeoutMs = integer('lockTimeoutMs', options.lockTimeoutMs ?? SAGA_DEFAULTS.lockTimeoutMs, 1);
  const attempts = integer('attempts', options.attempts ?? SAGA_DEFAULTS.transactionAttempts, 1);
  const rollbackTimeoutMs = integer(
    'rollbackTimeoutMs',
    options.rollbackTimeoutMs ?? SAGA_DEFAULTS.rollbackTimeoutMs,
    1,
  );
  const idleInTransactionTimeoutMs = integer(
    'idleInTransactionTimeoutMs',
    options.idleInTransactionTimeoutMs ?? SAGA_DEFAULTS.idleInTransactionTimeoutMs,
    1,
  );
  const runner: SagaTransactionRunner = <T>(work: (tx: OutboxTransaction) => Promise<T>): Promise<T> =>
    transactionKeepingFailure<T, PoolDatabase, OutboxTransaction>(db as PoolDatabase, (tx) => work(tx), {
      lockTimeoutMs,
      idleInTransactionTimeoutMs,
      rollbackTimeoutMs,
      retry: { attempts, backoffMs: 50 },
    });
  sagaRunnerLimits.set(runner, { idleInTransactionTimeoutMs, lockTimeoutMs, rollbackTimeoutMs });
  return runner;
};
