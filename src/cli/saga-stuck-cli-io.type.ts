import type { OutboxExecutor } from '@hoshinotatsuya/outbox';

import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';

/** Окружение CLI разбора: переменные, вывод и подключение к базе (подменяются в проверках). */
export interface SagaStuckCliIo {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly stdout: (line: string) => void;
  readonly stderr: (line: string) => void;
  /** Чтение файла поправки контекста (`--context-patch-file`); по умолчанию — `fs.readFile` в UTF-8. */
  readonly readFile?: (path: string) => Promise<string>;
  readonly connect: (url: string) => Promise<{
    readonly executor: OutboxExecutor;
    readonly transaction: SagaTransactionRunner;
    close(): Promise<void>;
  }>;
}
