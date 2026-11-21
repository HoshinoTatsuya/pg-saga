import { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaJournalDraft } from '../types/saga-journal-draft.type';
import type { SagaReply } from '../types/saga-reply.type';
import type { SagaStepFacts } from '../types/saga-step-facts.type';

/**
 * Запись `late-reply` для УСПЕХА, которого экземпляр уже не ждёт, — или `null`.
 *
 * Поздний успех — событие, когда эффект у участника есть, а сага его не учла:
 * команда этой попытки и поколения действительно уходила (`dispatched`), а шаг
 * в этой фазе не выполнен НИ ОДНОЙ попыткой (`done` ложно). Тогда это успех
 * шага после начала отката (компенсация могла дойти до участника раньше),
 * успех компенсации у застрявшего (ручной возврат был бы вторым) или успех
 * поколения до ручного повтора. Иначе — не событие: дубль шага, уже
 * пройденного другой попыткой или поколением, у идущей, откатанной или
 * завершённой саги (участник по `stepKey` вернул тот же исход), и ответ на
 * команду, которой не было (компенсация у идущей или завершённой саги).
 *
 * Сообщение чужого воплощения сюда не доходит: его отсекает сверка id.
 */
export const sagaLateReply = (
  instance: SagaInstance,
  reply: SagaReply,
  facts: SagaStepFacts,
): SagaJournalDraft | null => {
  if (!facts.dispatched || facts.done) {
    return null;
  }
  const rolledBack = (
    [SAGA_STATUS.compensating, SAGA_STATUS.compensated, SAGA_STATUS.stuck] as string[]
  ).includes(instance.status);
  return {
    event: SAGA_JOURNAL_EVENT.lateReply,
    phase: reply.phase,
    stepIndex: reply.stepIndex,
    step: instance.steps[reply.stepIndex] ?? null,
    attempt: reply.attempt,
    generation: reply.generation,
    detail:
      reply.phase === SAGA_PHASE.compensate
        ? `компенсация выполнена участником после того, как её перестали ждать; состояние экземпляра — ${instance.status}`
        : rolledBack
          ? `успех шага после начала отката; состояние экземпляра — ${instance.status}`
          : instance.status === SAGA_STATUS.resolved
            ? // Разобран вручную — из отката (stuck) или замёрзшим (running): «после отката» неверно для второго.
              'успех шага после ручного разбора экземпляра (resolved): эффект у участника есть, а сага его не учла'
            : reply.generation < instance.generation
              ? `успех поколения ${reply.generation} после ручного повтора (поколение ${instance.generation})`
              : `успех попытки ${reply.attempt}, которой экземпляр не ждёт; состояние экземпляра — ${instance.status}`,
  };
};
