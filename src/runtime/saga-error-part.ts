import { SAGA_ERROR_TEXT } from './saga-error-text-patterns.constant';
import { sagaValueText } from './saga-value-text';

/**
 * Одна ошибка цепочки `cause` → строка для текста отказа: `[SQLSTATE] Имя: сообщение`
 * без хвоста `params`; обёртка запроса драйвера — `null` (её текст несёт параметры).
 * Ошибка узнаётся по форме (строка `message`), а не `instanceof`: у ошибки из
 * другого контекста (vm, вторая копия пакета) имя иначе терялось бы. Значение,
 * чтение которого бросает (Proxy), — текст, а не отказ.
 */
export const sagaErrorPart = (error: unknown): string | null => {
  try {
    const message = (error as { message?: unknown } | null)?.message;
    if (typeof message === 'string') {
      const text = message.replace(SAGA_ERROR_TEXT.paramsTail, '');
      if (SAGA_ERROR_TEXT.queryWrapper.test(text)) {
        return null;
      }
      const name = (error as { name?: unknown }).name;
      const code = (error as { code?: unknown }).code;
      const prefix = typeof code === 'string' && code.length > 0 ? `[${code}] ` : '';
      return `${prefix}${typeof name === 'string' && name.length > 0 ? `${name}: ` : ''}${text}`;
    }
  } catch {
    // Чтение полей бросило: остаётся текст самого значения.
  }
  return error === undefined ? null : sagaValueText(error);
};
