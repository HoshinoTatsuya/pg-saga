import { runInNewContext } from 'node:vm';

import { DrizzleQueryError } from 'drizzle-orm';

import { sagaErrorText } from './saga-error-text';

describe('sagaErrorText', () => {
  it('отказ запроса: код и сообщение сервера из cause, SQL и параметры драйвера — нет', () => {
    const server = Object.assign(new Error('canceling statement due to lock timeout'), { code: '55P03' });
    const wrapper = new Error('Failed query: UPDATE accounts SET owner = $1\nparams: Иван Петров,4276-0000', {
      cause: server,
    });
    const text = sagaErrorText(wrapper);
    expect(text).toBe('[55P03] Error: canceling statement due to lock timeout');
    expect(text).not.toContain('Иван');
    expect(text).not.toContain('params');
  });

  it('формат обёртки drizzle закреплён: настоящий DrizzleQueryError — код и сообщение сервера, без SQL и params', () => {
    // Сменится формат при обновлении drizzle — красная проверка здесь, а не ПДн в журнале.
    const server = Object.assign(new Error('canceling statement due to lock timeout'), { code: '55P03' });
    const text = sagaErrorText(new DrizzleQueryError('SELECT * FROM t WHERE email = $1', ['a@b.c'], server));
    expect(text).toBe('[55P03] Error: canceling statement due to lock timeout');
  });

  it('хвост params срезается у любого сообщения; отказ без описания не выдаётся за скрытый текст драйвера', () => {
    expect(sagaErrorText(new Error('другой драйвер: SELECT $1\n params: 4276-0000'))).toBe(
      'Error: другой драйвер: SELECT $1',
    );
    expect([sagaErrorText(undefined), sagaErrorText(null)]).toEqual([
      'отказ без описания',
      'отказ без описания',
    ]);
  });

  it('только обёртка без причины — нейтральный текст, а не SQL с параметрами', () => {
    expect(sagaErrorText(new Error('Failed query: SELECT 1\nparams: секрет'))).not.toContain('секрет');
  });

  it('цепочка причин — через стрелку; обычный отказ — имя и сообщение', () => {
    const root = new TypeError('нет поля');
    expect(sagaErrorText(new Error('шаг упал', { cause: root }))).toBe(
      'Error: шаг упал ← TypeError: нет поля',
    );
    expect(sagaErrorText('строка')).toBe('строка');
  });

  it('NUL вычищен; обрезка по потолку не режет суррогатную пару', () => {
    expect(sagaErrorText(new Error('a\u0000b'))).toBe('Error: ab');
    const emoji = '😀'.repeat(10);
    const cut = sagaErrorText(emoji, 6);
    expect(cut.endsWith('…')).toBe(true);
    // Остаток без висящей половины пары: каждая пара целая.
    expect(Array.from(cut.slice(0, -1)).every((ch) => ch === '😀')).toBe(true);
    expect(sagaErrorText('x'.repeat(10), 5)).toBe('xxxx…');
  });
});

describe('sagaErrorText: значение, которое String() не берёт, и непарный суррогат', () => {
  it.each([
    ['объект без прототипа', () => Object.create(null)],
    [
      'toString бросает',
      () => ({
        toString() {
          throw new Error('нет');
        },
      }),
    ],
    [
      'Proxy, бросающий на любом чтении',
      () =>
        new Proxy(
          {},
          {
            get() {
              throw new Error('ловушка');
            },
            getPrototypeOf() {
              throw new Error('ловушка');
            },
          },
        ),
    ],
    ['BigInt-поле и цикл без прототипа', () => Object.assign(Object.create(null), { n: 1n, self: null })],
  ])('%s — текст, а не TypeError', (_name, make) => {
    const value = make();
    expect(() => sagaErrorText(value)).not.toThrow();
    expect(sagaErrorText(value).length).toBeGreaterThan(0);
    // В причине тоже: обход цепочки не ломается на ней.
    expect(sagaErrorText(new Error('шаг упал', { cause: value }))).toMatch(/^Error: шаг упал ← /);
  });

  it('ошибка из другого контекста (vm): имя сохраняется', () => {
    const foreign = runInNewContext('new TypeError("чужой")') as unknown;
    expect(foreign instanceof Error).toBe(false);
    expect(sagaErrorText(foreign)).toBe('TypeError: чужой');
  });

  it('непарный суррогат (обрезка посреди эмодзи) — заменён: jsonb примет текст', () => {
    const cut = 'отказ по вводу 😀'.slice(0, -1);
    expect(cut.isWellFormed()).toBe(false);
    const text = sagaErrorText(new Error(cut));
    expect({ wellFormed: text.isWellFormed(), text }).toEqual({
      wellFormed: true,
      text: 'Error: отказ по вводу �',
    });
    expect(sagaErrorText('x\udc00y\u0000').isWellFormed()).toBe(true);
    expect(sagaErrorText('x\udc00y\u0000')).toBe('x�y');
  });
});
