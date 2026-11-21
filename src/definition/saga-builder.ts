import { integer } from '../config/config-integer';
import { sagaName } from '../config/config-name';
import { resolveRetry } from '../config/resolve-retry';
import { SAGA_DEFAULTS } from '../constants/saga-defaults.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';
import type { SagaContextMerge } from '../types/saga-context-merge.type';
import type { SagaDefinition } from '../types/saga-definition.type';
import type { SagaLocalStepSpec } from '../types/saga-local-step-spec.type';
import type { SagaRemoteStepSpec } from '../types/saga-remote-step-spec.type';
import type { SagaRetryOptions } from '../types/saga-retry-options.type';
import type { SagaStep } from '../types/saga-step.type';
import type { SagaStepOutput } from '../types/saga-step-output.type';

/**
 * Построитель определения: каждый шаг возвращает НОВЫЙ построитель, чей
 * контекст — прежний плюс выход шага. Следующий шаг видит в `context` поля
 * всех предыдущих, и обращение к полю, которого шаги не дают, не
 * компилируется. Построитель неизменяем: ветка от общего префикса не портит
 * соседнюю.
 */
export class SagaBuilder<Input extends object, Ctx extends object> {
  public constructor(
    private readonly _name: string,
    private readonly _version: number,
    private readonly _steps: readonly SagaStep[],
  ) {}

  /** Локальный шаг: выполняется в транзакции продвижения саги. */
  // biome-ignore lint/suspicious/noConfusingVoidType: шаг без выхода объявляется `async () => {}` — это Promise<void>, не Promise<undefined>
  public step<Out extends object | void = void>(
    name: string,
    spec: SagaLocalStepSpec<Ctx, Out>,
  ): SagaBuilder<Input, SagaContextMerge<Ctx, SagaStepOutput<Out>>> {
    if (typeof spec?.execute !== 'function') {
      throw new SagaConfigError(`step ${sagaValueText(name)}`, 'нужна функция execute');
    }
    if (spec.compensate !== undefined && typeof spec.compensate !== 'function') {
      throw new SagaConfigError(`step ${sagaValueText(name)}`, 'compensate — функция');
    }
    return this._with({
      name: this._stepName(name),
      kind: 'local',
      execute: spec.execute as SagaStep['execute'],
      compensate: (spec.compensate ?? null) as SagaStep['compensate'],
      command: null,
      compensation: null,
      ...this._retries(name, spec),
      // Без своего потолка — потолок по умолчанию, а не «без потолка»: шаг
      // дольше доставки релея иначе молча замораживал бы экземпляр.
      timeoutMs:
        spec.timeoutMs === undefined
          ? SAGA_DEFAULTS.localStepTimeoutMs
          : integer(`step ${name}.timeoutMs`, spec.timeoutMs, 1),
    });
  }

  /** Удалённый шаг: команда участнику через outbox, ответ — `handleReply`. */
  public remoteStep<Out extends object = Record<never, never>>(
    name: string,
    spec: SagaRemoteStepSpec<Ctx, Out>,
  ): SagaBuilder<Input, SagaContextMerge<Ctx, Out>> {
    if (typeof spec?.command !== 'function') {
      throw new SagaConfigError(`remoteStep ${sagaValueText(name)}`, 'нужна функция command');
    }
    if (spec.compensation !== undefined && typeof spec.compensation !== 'function') {
      throw new SagaConfigError(`remoteStep ${sagaValueText(name)}`, 'compensation — функция');
    }
    return this._with({
      name: this._stepName(name),
      kind: 'remote',
      execute: null,
      compensate: null,
      command: spec.command as SagaStep['command'],
      compensation: (spec.compensation ?? null) as SagaStep['compensation'],
      ...this._retries(name, spec),
      timeoutMs: integer(`remoteStep ${name}.timeoutMs`, spec.timeoutMs, 1),
    });
  }

  public build(): SagaDefinition<Input, Ctx> {
    if (this._steps.length === 0) {
      throw new SagaConfigError(`saga ${this._name}`, 'нет ни одного шага');
    }
    return Object.freeze({
      name: this._name,
      version: this._version,
      steps: Object.freeze([...this._steps]),
    });
  }

  private _stepName(name: unknown): string {
    const checked = sagaName('step', name);
    if (this._steps.some((step) => step.name === checked)) {
      throw new SagaConfigError(`step ${checked}`, `шаг с этим именем в саге ${this._name} уже есть`);
    }
    return checked;
  }

  private _retries(
    name: string,
    spec: { readonly retry?: SagaRetryOptions; readonly compensationRetry?: SagaRetryOptions },
  ): Pick<SagaStep, 'retry' | 'compensationRetry'> {
    return {
      retry: resolveRetry(`step ${name}.retry`, spec.retry, SAGA_DEFAULTS.stepRetry),
      compensationRetry: resolveRetry(
        `step ${name}.compensationRetry`,
        spec.compensationRetry,
        SAGA_DEFAULTS.compensationRetry,
      ),
    };
  }

  private _with<Next extends object>(step: SagaStep): SagaBuilder<Input, Next> {
    return new SagaBuilder<Input, Next>(this._name, this._version, [...this._steps, Object.freeze(step)]);
  }
}
