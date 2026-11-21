/**
 * Отказ записи команды в outbox — ДЕТЕРМИНИРОВАННЫЙ: повторная доставка той же
 * команды откажет точно так же. Такой отказ — отказ шага (без повторов,
 * компенсации), а не отказ доставки: тот повторялся бы до DEAD, оставляя
 * экземпляр навсегда running.
 *
 * Детерминированный — это отказ ПРОВЕРКИ события до записи (outbox бросает
 * `TypeError`/`RangeError` с префиксом `outbox` до первого запроса: не JSON, NUL, payload больше
 * `maxPayloadBytes`, заголовки без опции `headers` или не той формы, id у типа
 * со слиянием) и отказ ДАННЫХ на сервере (SQLSTATE класса 22: строка длиннее
 * `varchar(64)` у `event_type`/`aggregate_type` и прочие data exception).
 * Всё остальное — отказ базы или связи (обрыв, блокировка, сериализация,
 * отказ базы без SQLSTATE) — уходит наружу: доставку повторит outbox.
 */
export const sagaEnqueueRejected = (error: unknown): boolean => {
  let database = false;
  let validation = false;
  let current: unknown = error;
  for (let depth = 0; depth < 8 && current !== null && typeof current === 'object'; depth += 1) {
    const record = current as {
      code?: unknown;
      name?: unknown;
      message?: unknown;
      query?: unknown;
      errno?: unknown;
    };
    if (typeof record.code === 'string' && /^[0-9A-Z]{5}$/.test(record.code)) {
      return record.code.startsWith('22');
    }
    // Отказ базы без SQLSTATE (обрыв соединения): обёртка outbox, drizzle или
    // системная ошибка сокета — не проверка события.
    if (record.name === 'OutboxDatabaseError' || 'query' in record || record.errno !== undefined) {
      database = true;
    }
    // Проверка события outbox — где угодно в цепочке (своя обёртка над
    // outbox заворачивает её в cause) и по форме: TypeError из другой копии
    // или контекста `instanceof` не узнал бы.
    if (
      (record.name === 'TypeError' || record.name === 'RangeError') &&
      typeof record.message === 'string' &&
      /^outbox[.:]/.test(record.message)
    ) {
      validation = true;
    }
    current = (current as { cause?: unknown }).cause;
  }
  // Проверка события — это отказ САМОГО outbox до записи: его TypeError и
  // RangeError подписаны префиксом `outbox`. Голый TypeError (сокет своей
  // обёртки над outbox, ошибка кода) детерминированным не считается: отказ шага
  // стоил бы попытки и компенсаций, а повтор доставки — только времени.
  return !database && validation;
};
