import { sagaEnqueueRejected } from './saga-enqueue-rejected';

/** Отказ базы так, как его отдаёт outbox: `OutboxDatabaseError` с SQLSTATE (или без) и причиной. */
const outboxDatabaseError = (code: string | undefined): Error =>
  Object.assign(new Error('отказ базы', { cause: Object.assign(new Error('сервер'), { code }) }), {
    name: 'OutboxDatabaseError',
    operation: 'enqueue',
    code,
  });

/** Обёртка drizzle над отказом драйвера: SQL и параметры, причина — в cause. */
const drizzleWrapped = (code: string): Error =>
  Object.assign(new Error('Failed query: insert …'), {
    query: 'insert …',
    params: [],
    cause: Object.assign(new Error('value too long'), { code }),
  });

describe('sagaEnqueueRejected: какой отказ записи команды — детерминированный', () => {
  it.each([
    [
      'проверка события outbox (TypeError: не JSON, NUL, заголовки)',
      new TypeError('outbox: payload содержит NUL'),
      true,
    ],
    [
      'проверка события outbox (RangeError: maxPayloadBytes, headers без опции)',
      new RangeError('outbox: payload 1100299 байт больше потолка maxPayloadBytes 1048576'),
      true,
    ],
    [
      'проверка enqueue outbox (RangeError: заголовки без опции)',
      new RangeError('outbox.enqueue: заголовки события без опции headers'),
      true,
    ],
    // Голый TypeError без признаков базы — сокет своей обёртки над outbox или
    // ошибка кода: не проверка события, повторит доставка.
    [
      'голый TypeError (сокет своей обёртки)',
      new TypeError("Cannot read properties of null (reading 'write')"),
      false,
    ],
    ['голый RangeError кода', new RangeError('Invalid array length'), false],
    ['строка длиннее varchar(64) — 22001 у outbox', outboxDatabaseError('22001'), true],
    ['данные не той формы на сервере — 22P02 под обёрткой drizzle', drizzleWrapped('22P02'), true],
    ['непредставимый символ — 22P05', outboxDatabaseError('22P05'), true],
    // Двойники: отказ базы или связи — повторит доставка.
    ['сериализация — 40001', outboxDatabaseError('40001'), false],
    ['блокировка — 55P03', outboxDatabaseError('55P03'), false],
    ['обрыв соединения — 08006', drizzleWrapped('08006'), false],
    ['NOT NULL — 23502 (не данные, а форма вызова)', outboxDatabaseError('23502'), false],
    ['отказ базы без SQLSTATE (обрыв)', outboxDatabaseError(undefined), false],
    [
      'сокет: ECONNRESET',
      Object.assign(new TypeError('read ECONNRESET'), { errno: -54, code: 'ECONNRESET' }),
      false,
    ],
    ['прочая ошибка кода', new Error('что-то иное'), false],
  ])('%s → %s', (_name, error, expected) => {
    expect(sagaEnqueueRejected(error)).toBe(expected);
  });
});
