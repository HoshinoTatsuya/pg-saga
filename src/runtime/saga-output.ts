import { SAGA_LOSSY_TAGS } from '../constants/saga-lossy-tags.constant';
import { SagaPayloadError } from '../errors/saga-payload.error';
import type { SagaContext } from '../types/saga-context.type';
import { sagaJsonHasLoneSurrogate } from './saga-json-has-lone-surrogate';
import { sagaJsonHasNul } from './saga-json-has-nul';
import { sagaValueText } from './saga-value-text';

/**
 * Выход шага → объект для контекста: ничего (`undefined`/`null`) — пустой
 * объект; объект — через JSON (то, что увидит следующий шаг после чтения из
 * базы, — ровно то же, что видит этот переход). Что ПОСЛЕ JSON не объект —
 * массив, примитив, а также объект с `toJSON()`, отдающим не объект (`Date`
 * становится строкой, и та разложилась бы в контексте на ключи «0»…«23»), —
 * и BigInt, NUL и непарный суррогат в строке (jsonb их не хранит), и Map, Set, Error где угодно
 * внутри (JSON молча превращает их в `{}`) — отказ шага без повторов.
 */
export const sagaOutput = (what: string, output: unknown): SagaContext => {
  if (output === undefined || output === null) {
    return {};
  }
  if (typeof output !== 'object' || Array.isArray(output)) {
    throw new SagaPayloadError(`${what}: выход шага — объект или ничего, получено ${typeof output}`);
  }
  let text: string | undefined;
  try {
    text = JSON.stringify(output, (key, value: unknown) => {
      // Контейнеры и объекты, чьё содержимое JSON теряет МОЛЧА: Map и Set
      // становятся `{}`, у Error пропадают сообщение и стек (поля
      // неперечислимые). Следующий шаг получил бы пустой объект без отказа.
      const tag = typeof value === 'object' && value !== null ? Object.prototype.toString.call(value) : '';
      if (SAGA_LOSSY_TAGS.includes(tag) || ArrayBuffer.isView(value)) {
        throw new SagaPayloadError(
          `${what}: ${key === '' ? 'выход шага' : `поле ${key}`} — ${tag.slice(8, -1)}, JSON теряет его содержимое; переведите в объект или массив`,
        );
      }
      return value;
    });
  } catch (error) {
    if (error instanceof SagaPayloadError) {
      throw error;
    }
    throw new SagaPayloadError(`${what}: выход шага не сериализуется в JSON (${sagaValueText(error)})`);
  }
  if (text === undefined) {
    // `toJSON()`, вернувший undefined: иначе — TypeError ниже вместо отказа шага.
    throw new SagaPayloadError(`${what}: выход шага не представим в JSON (toJSON вернул undefined)`);
  }
  if (sagaJsonHasNul(text)) {
    throw new SagaPayloadError(`${what}: NUL в строке выхода — jsonb его не хранит`);
  }
  if (sagaJsonHasLoneSurrogate(text)) {
    throw new SagaPayloadError(
      `${what}: непарная половина суррогатной пары в строке выхода (обрезка посреди эмодзи?) — jsonb её не хранит`,
    );
  }
  const parsed: unknown = JSON.parse(text);
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new SagaPayloadError(
      `${what}: выход шага в JSON — не объект (${Array.isArray(parsed) ? 'массив' : typeof parsed}; toJSON?)`,
    );
  }
  return parsed as SagaContext;
};
