import { SagaPayloadError } from '../errors/saga-payload.error';
import { sagaJsonHasNul } from './saga-json-has-nul';
import { sagaOutput } from './saga-output';

describe('sagaOutput', () => {
  it('toJSON(), вернувший undefined, — отказ шага (SagaPayloadError), а не TypeError', () => {
    expect(() => sagaOutput('шаг x', { toJSON: () => undefined })).toThrow(SagaPayloadError);
  });

  it('объект — копия через JSON; ничего — пустой объект; массив, BigInt, NUL — отказ', () => {
    expect(sagaOutput('шаг x', { at: new Date('2026-11-16T00:00:00.000Z') })).toEqual({
      at: '2026-11-16T00:00:00.000Z',
    });
    expect(sagaOutput('шаг x', undefined)).toEqual({});
    for (const bad of [[1], { n: 1n }, { s: 'a\u0000b' }]) {
      expect(() => sagaOutput('шаг x', bad)).toThrow(SagaPayloadError);
    }
  });

  it('объект, чей toJSON отдаёт НЕ объект (Date целиком, массив, число, строку, null), — отказ, а не ключи «0»…«23»', () => {
    for (const bad of [
      new Date('2026-11-01T00:00:00.000Z'),
      { toJSON: () => [1, 2] },
      { toJSON: () => 7 },
      { toJSON: () => 'текст' },
      { toJSON: () => null },
    ]) {
      expect(() => sagaOutput('шаг x', bad)).toThrow(/не объект/);
    }
    // Двойник: toJSON, отдающий объект, — его поля.
    expect(sagaOutput('шаг x', { toJSON: () => ({ a: 1 }) })).toEqual({ a: 1 });
  });

  it.each([
    ['Map в поле', { prices: new Map([['a', 1]]) }, /поле prices — Map/],
    ['Set в поле', { tags: new Set(['a']) }, /поле tags — Set/],
    ['Error в поле (сообщение и стек пропали бы)', { cause: new Error('склад лежит') }, /поле cause — Error/],
    ['наследник Error', { cause: new RangeError('x') }, /поле cause — Error/],
    ['Map глубоко внутри', { a: { b: [{ c: new Map() }] } }, /поле c — Map/],
    ['Map целиком', new Map([['a', 1]]), /выход шага — Map/],
    ['WeakMap', { w: new WeakMap() }, /поле w — WeakMap/],
    ['Promise (забытый await)', { p: Promise.resolve(1) }, /поле p — Promise/],
  ])('%s — отказ шага, а не молчаливый {}', (_name, value, message) => {
    expect(() => sagaOutput('шаг x', value)).toThrow(SagaPayloadError);
    expect(() => sagaOutput('шаг x', value)).toThrow(message);
  });

  it('двойники: экземпляр класса с полями, пустой объект, Date в поле — проходят', () => {
    class Charge {
      public constructor(public readonly chargeId: string) {}
    }
    expect({
      entity: sagaOutput('шаг x', { charge: new Charge('c-1') }),
      empty: sagaOutput('шаг x', { nothing: {} }),
      date: sagaOutput('шаг x', { at: new Date('2026-11-01T00:00:00.000Z') }),
    }).toEqual({
      entity: { charge: { chargeId: 'c-1' } },
      empty: { nothing: {} },
      date: { at: '2026-11-01T00:00:00.000Z' },
    });
  });

  it('буквальный текст «\\u0000» (обратная косая и пять знаков, не NUL) — не отказ: jsonb его хранит', () => {
    expect(sagaOutput('шаг x', { pattern: '\\u0000', path: 'C:\\u0000dir' })).toEqual({
      pattern: '\\u0000',
      path: 'C:\\u0000dir',
    });
  });
});

describe('sagaJsonHasNul', () => {
  it.each([
    ['NUL', { s: '\u0000' }, true],
    ['NUL после буквальной обратной косой', { s: '\\\u0000' }, true],
    ['NUL в ключе', { 'a\u0000': 1 }, true],
    ['буквальный \\u0000', { s: '\\u0000' }, false],
    ['две обратные косые и u0000', { s: '\\\\u0000' }, false],
    ['без NUL', { s: 'u0000' }, false],
  ])('%s', (_name, value, expected) => {
    expect(sagaJsonHasNul(JSON.stringify(value))).toBe(expected);
  });
});

describe('sagaOutput: непарный суррогат', () => {
  it('половина суррогатной пары в строке или ключе — отказ шага (jsonb её не хранит); целая пара и буквальный текст «\\ud83d» — нет', () => {
    const cut = 'эмодзи 😀'.slice(0, -1);
    for (const bad of [{ s: cut }, { [cut]: 1 }, { deep: [{ s: '\udc00' }] }]) {
      expect(() => sagaOutput('шаг x', bad)).toThrow(/суррогат/);
    }
    expect(sagaOutput('шаг x', { s: 'эмодзи 😀', literal: '\\ud83d' })).toEqual({
      s: 'эмодзи 😀',
      literal: '\\ud83d',
    });
  });
});
