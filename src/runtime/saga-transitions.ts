import { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import type { SagaContext } from '../types/saga-context.type';
import type { SagaDefinition } from '../types/saga-definition.type';
import type { SagaFailure } from '../types/saga-failure.type';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaPhase } from '../types/saga-phase.type';
import type { SagaSignal } from '../types/saga-signal.type';
import type { SagaStep } from '../types/saga-step.type';
import type { SagaTransition } from '../types/saga-transition.type';
import { sagaBackoffMs } from './saga-backoff-ms';
import { sagaErrorText } from './saga-error-text';

/**
 * Переходы экземпляра — чистые функции: по определению, экземпляру и исходу
 * шага — новое состояние, записи журнала, следующая команда и сигналы метрик.
 * Пишет их оркестратор одной транзакцией.
 */
export class SagaTransitions {
  /** Фаза, которую ждёт экземпляр: выполнение у `running`, компенсация у остальных. */
  public static phaseOf(instance: SagaInstance): SagaPhase {
    return instance.status === SAGA_STATUS.running ? SAGA_PHASE.execute : SAGA_PHASE.compensate;
  }

  /**
   * Ближайший к `from` шаг (включительно, вниз к нулю), которому есть что
   * компенсировать; нет — `-1`. Компенсации идут СТРОГО в обратном порядке
   * завершения: каждый следующий поиск начинается ниже компенсированного.
   */
  public static compensationFrom(definition: SagaDefinition, from: number): number {
    for (let index = Math.min(from, definition.steps.length - 1); index >= 0; index -= 1) {
      const step = definition.steps[index];
      if (step !== undefined && (step.compensate !== null || step.compensation !== null)) {
        return index;
      }
    }
    return -1;
  }

  /** Шаг или компенсация выполнены. */
  public static success(
    definition: SagaDefinition,
    instance: SagaInstance,
    output: SagaContext,
    now: Date,
  ): SagaTransition {
    const index = instance.stepIndex;
    const step = SagaTransitions._step(definition, index);
    const base = {
      phase: SagaTransitions.phaseOf(instance),
      stepIndex: index,
      step: step.name,
      attempt: instance.attempt,
      generation: instance.generation,
      detail: null,
    };
    const stepDone: SagaSignal = {
      kind: 'stepCompleted',
      event: {
        saga: instance.name,
        sagaId: instance.id,
        step: step.name,
        phase: base.phase,
        attempt: instance.attempt,
        sinceLastMoveMs: Math.max(0, now.getTime() - instance.updatedAt.getTime()),
      },
    };
    if (instance.status === SAGA_STATUS.running) {
      const context = { ...instance.context, ...output };
      const completed = { ...base, event: SAGA_JOURNAL_EVENT.stepCompleted };
      if (index + 1 >= definition.steps.length) {
        return {
          state: {
            ...SagaTransitions._state(instance),
            status: SAGA_STATUS.completed,
            context,
            lastError: null,
            finishedAt: now,
          },
          journal: [completed, SagaTransitions._sagaEvent(instance, SAGA_JOURNAL_EVENT.sagaCompleted, null)],
          command: null,
          signals: [
            stepDone,
            {
              kind: 'completed',
              event: {
                saga: instance.name,
                sagaId: instance.id,
                durationMs: now.getTime() - instance.createdAt.getTime(),
              },
            },
          ],
        };
      }
      return {
        state: {
          ...SagaTransitions._state(instance),
          stepIndex: index + 1,
          attempt: 1,
          context,
          lastError: null,
        },
        journal: [completed],
        command: {
          phase: SAGA_PHASE.execute,
          stepIndex: index + 1,
          attempt: 1,
          generation: instance.generation,
          delayMs: null,
        },
        signals: [stepDone],
      };
    }
    const compensated = { ...base, event: SAGA_JOURNAL_EVENT.stepCompensated };
    return SagaTransitions._compensateNext(
      definition,
      instance,
      index - 1,
      [compensated],
      [stepDone],
      now,
      null,
    );
  }

  /**
   * Отмена идущей саги вызывающим: новое поколение (сообщения прежнего — его
   * команда, таймер, поздний ответ — больше не применяются) и компенсации
   * выполненных шагов в обратном порядке. Текущий локальный шаг не выполнен
   * (его транзакция не зафиксирована) — компенсировать нечего; текущий
   * УДАЛЁННЫЙ — исход неизвестен (команда могла уйти), компенсируется и он, а
   * без компенсации — запись `uncompensated-uncertain`, как при таймауте.
   *
   * Компенсировать нечего — сага завершается здесь же, в транзакции
   * вызывающего, где хука быть не может (она может откатиться). Тогда вместо
   * сигнала — команда-уведомление с адресом завершённого экземпляра:
   * оркестратор вызовет хук `compensated` после фиксации своей транзакции.
   */
  public static cancel(
    definition: SagaDefinition,
    instance: SagaInstance,
    reason: string,
    now: Date,
  ): SagaTransition {
    const step = SagaTransitions._step(definition, instance.stepIndex);
    const failure = sagaErrorText(`отменено: ${reason}`);
    const next = { ...instance, generation: instance.generation + 1 };
    const remote = step.kind === 'remote';
    const unprotected =
      remote && step.compensate === null && step.compensation === null
        ? [
            {
              event: SAGA_JOURNAL_EVENT.uncompensatedUncertain,
              phase: SAGA_PHASE.execute,
              stepIndex: instance.stepIndex,
              step: step.name,
              attempt: instance.attempt,
              generation: instance.generation,
              detail: `сага отменена, когда команда шага ${step.name} могла уйти, а компенсации у него нет — проверьте эффект у участника`,
            },
          ]
        : [];
    const transition = SagaTransitions._compensateNext(
      definition,
      next,
      remote ? instance.stepIndex : instance.stepIndex - 1,
      [SagaTransitions._sagaEvent(next, SAGA_JOURNAL_EVENT.sagaCancelled, failure), ...unprotected],
      [],
      now,
      failure,
    );
    if (transition.state.status !== SAGA_STATUS.compensated) {
      return { ...transition, signals: [] };
    }
    return {
      ...transition,
      command: {
        phase: SAGA_PHASE.compensate,
        stepIndex: transition.state.stepIndex,
        attempt: transition.state.attempt,
        generation: transition.state.generation,
        delayMs: null,
      },
      signals: [],
    };
  }

  /** Шаг или компенсация отказали. */
  public static failure(
    definition: SagaDefinition,
    instance: SagaInstance,
    rawFailure: SagaFailure,
    now: Date,
    random: () => number,
  ): SagaTransition {
    // Текст — через ту же обрезку и чистку, что у всех вызывающих: новый
    // вызывающий без `sagaErrorText` не переполнит журнал и не пронесёт NUL.
    const failure = { ...rawFailure, error: sagaErrorText(rawFailure.error) };
    const index = instance.stepIndex;
    const step = SagaTransitions._step(definition, index);
    const phase = SagaTransitions.phaseOf(instance);
    const executing = phase === SAGA_PHASE.execute;
    const retry = executing ? step.retry : step.compensationRetry;
    const willRetry = failure.retryable && instance.attempt < retry.maxAttempts;
    const entry = {
      event: executing
        ? failure.timedOut
          ? SAGA_JOURNAL_EVENT.stepTimedOut
          : SAGA_JOURNAL_EVENT.stepFailed
        : SAGA_JOURNAL_EVENT.compensationFailed,
      phase,
      stepIndex: index,
      step: step.name,
      attempt: instance.attempt,
      generation: instance.generation,
      detail: failure.error,
    };
    const signal: SagaSignal = {
      kind: 'stepFailed',
      event: {
        saga: instance.name,
        sagaId: instance.id,
        step: step.name,
        phase,
        attempt: instance.attempt,
        willRetry,
        error: failure.error,
      },
    };
    if (willRetry) {
      return {
        state: {
          ...SagaTransitions._state(instance),
          attempt: instance.attempt + 1,
          lastError: failure.error,
        },
        journal: [entry],
        command: {
          phase,
          stepIndex: index,
          attempt: instance.attempt + 1,
          generation: instance.generation,
          delayMs: sagaBackoffMs(retry, instance.attempt, random),
        },
        signals: [signal],
      };
    }
    if (!executing) {
      // Компенсация исчерпала повторы: дальше — только человек. Экземпляр
      // остаётся на этом шаге, чтобы ручной повтор продолжил с него же.
      return {
        state: { ...SagaTransitions._state(instance), status: SAGA_STATUS.stuck, lastError: failure.error },
        journal: [entry, SagaTransitions._sagaEvent(instance, SAGA_JOURNAL_EVENT.sagaStuck, failure.error)],
        command: null,
        signals: [
          signal,
          { kind: 'stuck', event: { saga: instance.name, sagaId: instance.id, error: failure.error } },
        ],
      };
    }
    // Исход неизвестен, а отменить шаг нечем: компенсации пройдут мимо него
    // молча, хотя участник мог шаг выполнить. Человеку это надо видеть.
    const unprotected =
      failure.uncertain && step.compensate === null && step.compensation === null
        ? [
            {
              ...entry,
              event: SAGA_JOURNAL_EVENT.uncompensatedUncertain,
              detail: `исход шага ${step.name} неизвестен, а компенсации у него нет — проверьте эффект у участника`,
            },
          ]
        : [];
    return SagaTransitions._compensateNext(
      definition,
      { ...instance, failure: failure.error, lastError: failure.error },
      failure.uncertain ? index : index - 1,
      [entry, ...unprotected],
      [signal],
      now,
      failure.error,
    );
  }

  /** Следующая компенсация ниже `from` (включительно) или конец саги компенсациями. */
  private static _compensateNext(
    definition: SagaDefinition,
    instance: SagaInstance,
    from: number,
    journal: SagaTransition['journal'],
    signals: readonly SagaSignal[],
    now: Date,
    failure: string | null,
  ): SagaTransition {
    const next = SagaTransitions.compensationFrom(definition, from);
    const state = {
      ...SagaTransitions._state(instance),
      failure: failure ?? instance.failure,
      lastError: failure,
    };
    if (next < 0) {
      return {
        state: { ...state, status: SAGA_STATUS.compensated, finishedAt: now },
        journal: [
          ...journal,
          SagaTransitions._sagaEvent(instance, SAGA_JOURNAL_EVENT.sagaCompensated, state.failure),
        ],
        command: null,
        signals: [
          ...signals,
          {
            kind: 'compensated',
            event: { saga: instance.name, sagaId: instance.id, failure: state.failure },
          },
        ],
      };
    }
    return {
      state: { ...state, status: SAGA_STATUS.compensating, stepIndex: next, attempt: 1 },
      journal,
      command: {
        phase: SAGA_PHASE.compensate,
        stepIndex: next,
        attempt: 1,
        generation: instance.generation,
        delayMs: null,
      },
      signals,
    };
  }

  private static _state(instance: SagaInstance): SagaTransition['state'] {
    return {
      status: instance.status,
      stepIndex: instance.stepIndex,
      attempt: instance.attempt,
      generation: instance.generation,
      context: instance.context,
      failure: instance.failure,
      lastError: instance.lastError,
      finishedAt: instance.finishedAt,
    };
  }

  private static _sagaEvent(
    instance: SagaInstance,
    event: SagaTransition['journal'][number]['event'],
    detail: string | null,
  ): SagaTransition['journal'][number] {
    return {
      event,
      phase: null,
      stepIndex: null,
      step: null,
      attempt: null,
      generation: instance.generation,
      detail,
    };
  }

  private static _step(definition: SagaDefinition, index: number): SagaStep {
    const step = definition.steps[index];
    if (step === undefined) {
      throw new Error(`pg-saga: у саги ${definition.name} нет шага ${index}`);
    }
    return step;
  }
}
