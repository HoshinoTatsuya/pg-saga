import { SAGA_DEFAULTS } from '../constants/saga-defaults.constant';
import { sagaErrorPart } from './saga-error-part';
import { SAGA_ERROR_TEXT } from './saga-error-text-patterns.constant';

/**
 * Текст отказа для журнала и `last_error`: сообщение и код (SQLSTATE) по цепочке
 * `cause` — обёртка драйвера с SQL и параметрами пропускается, причина остаётся.
 * Без NUL и без непарных суррогатов (U+FFFD вместо них), обрезано по потолку,
 * суррогатная пара не разрезается. Не бросает ни на каком значении.
 */
export const sagaErrorText = (error: unknown, max: number = SAGA_DEFAULTS.errorMaxLength): string => {
  const parts: string[] = [];
  let current: unknown = error;
  for (
    let depth = 0;
    depth < SAGA_ERROR_TEXT.causeDepth && current !== undefined && current !== null;
    depth += 1
  ) {
    const part = sagaErrorPart(current);
    if (part !== null && part.length > 0) {
      parts.push(part);
    }
    try {
      current = (current as { cause?: unknown }).cause;
    } catch {
      // Чтение причины бросило (Proxy): цепочка кончается здесь.
      current = undefined;
    }
  }
  // Пусто — либо была только обёртка запроса (её текст скрыт: несёт параметры),
  // либо отказ без описания (`abort()` без причины, `undefined`): не путать.
  let wrapperOnly: unknown;
  try {
    wrapperOnly = (error as { message?: unknown } | null)?.message;
  } catch {
    wrapperOnly = undefined;
  }
  const raw =
    parts.length > 0
      ? parts.join(' ← ')
      : typeof wrapperOnly === 'string' && SAGA_ERROR_TEXT.queryWrapper.test(wrapperOnly)
        ? 'отказ запроса (текст драйвера скрыт: несёт параметры)'
        : 'отказ без описания';
  // NUL и непарная половина суррогатной пары — jsonb не примет ни то, ни другое
  // (22P05/22P02 посреди транзакции перехода или ответа участника).
  const clean = raw.replaceAll('\u0000', '').toWellFormed();
  if (clean.length <= max) {
    return clean;
  }
  let cut = max - 1;
  const last = clean.charCodeAt(cut - 1);
  if (last >= 0xd800 && last <= 0xdbff) {
    cut -= 1;
  }
  return `${clean.slice(0, cut)}…`;
};
