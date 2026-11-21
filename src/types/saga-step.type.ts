import type { SagaRemoteArgs } from './saga-remote-args.type';
import type { SagaRemoteCommand } from './saga-remote-command.type';
import type { SagaResolvedRetry } from './saga-resolved-retry.type';
import type { SagaStepArgs } from './saga-step-args.type';

/** Шаг определения в том виде, в каком его исполняет оркестратор. */
export interface SagaStep {
  readonly name: string;
  readonly kind: 'local' | 'remote';
  readonly execute: ((args: SagaStepArgs<never>) => Promise<unknown>) | null;
  readonly compensate: ((args: SagaStepArgs<never>) => Promise<void>) | null;
  readonly command: ((args: SagaRemoteArgs<never>) => SagaRemoteCommand) | null;
  readonly compensation: ((args: SagaRemoteArgs<never>) => SagaRemoteCommand) | null;
  readonly retry: SagaResolvedRetry;
  readonly compensationRetry: SagaResolvedRetry;
  readonly timeoutMs: number | null;
}
