import type { SagaInstance } from '../types/saga-instance.type';

/**
 * Ключ идемпотентности шага у участника и во внешних системах. Формат 2 —
 * `<sagaId>#<воплощение>:<фаза>:<номер шага>`: id саги можно занять снова
 * после `prune`, и без воплощения участник по своему ключу вернул бы ИСХОД
 * ПРЕЖНЕЙ саги (например, «списано» по уже возвращённому платежу). Формат 1
 * (`<sagaId>:<фаза>:<номер>`) — у экземпляров, начатых версией 0.1.0: их ключ
 * не меняется посреди жизни, иначе повтор шага после выкатки пришёл бы к
 * участнику под новым ключом и выполнился второй раз.
 */
export const sagaStepKey = (
  instance: Pick<SagaInstance, 'id' | 'incarnation'>,
  phase: string,
  stepIndex: number,
): string =>
  instance.incarnation === null
    ? `${instance.id}:${phase}:${stepIndex}`
    : `${instance.id}#${instance.incarnation}:${phase}:${stepIndex}`;
