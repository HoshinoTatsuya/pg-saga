import { deepFreeze } from '../deep-freeze';
import type { SagaContext } from '../types/saga-context.type';

/**
 * Контекст для шага — замороженная копия. Шаг меняет контекст только своим
 * выходом: изменение вложенного поля на месте (`context.items.push(x)`) иначе
 * попало бы в сохранённый контекст мимо выхода шага (переход пишет
 * `{ ...context, ...output }` того же объекта). Контекст — JSON, копия — через JSON.
 */
export const sagaFrozenContext = (context: SagaContext): SagaContext => {
  const copy = JSON.parse(JSON.stringify(context)) as SagaContext;
  deepFreeze(copy);
  return copy;
};
