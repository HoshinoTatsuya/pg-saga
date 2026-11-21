import type { SagaContext } from './saga-context.type';
import type { SagaStep } from './saga-step.type';

/**
 * Определение саги (`defineSaga(...).step(...).build()`): имя, версия и шаги
 * по порядку. Экземпляр хранит версию и имена шагов, с которыми начат, и
 * доезжает по ним: определение другой версии к нему не применяется.
 */
export interface SagaDefinition<Input extends object = SagaContext, Ctx extends object = SagaContext> {
  readonly name: string;
  readonly version: number;
  readonly steps: readonly SagaStep[];
  /** Только для вывода типов: значения нет. */
  readonly types?: { readonly input: Input; readonly context: Ctx };
}
