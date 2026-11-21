import { SagaPayloadError } from '../errors/saga-payload.error';
import { sagaBackoffMs } from './saga-backoff-ms';
import { sagaOutput } from './saga-output';

describe('sagaBackoffMs и sagaOutput', () => {
  it('пауза: половина постоянная, половина случайная; экспонента до крышки', () => {
    const retry = { maxAttempts: 9, backoffMs: 100, maxBackoffMs: 500 };
    expect([1, 2, 3, 4, 5].map((attempt) => sagaBackoffMs(retry, attempt, () => 0))).toEqual([
      50, 100, 200, 250, 250,
    ]);
    expect([1, 2, 3, 4, 5].map((attempt) => sagaBackoffMs(retry, attempt, () => 1))).toEqual([
      100, 200, 400, 500, 500,
    ]);
    expect(sagaBackoffMs(retry, 1, () => 7)).toBe(100);
  });

  it('выход шага: ничего — пустой объект, объект — через JSON; массив, примитив, BigInt и NUL — отказ', () => {
    expect(sagaOutput('t', undefined)).toEqual({});
    expect(sagaOutput('t', { at: new Date('2026-11-01T00:00:00Z') })).toEqual({
      at: '2026-11-01T00:00:00.000Z',
    });
    for (const bad of [[1], 5, 'x', { n: 1n }, { text: 'a\u0000b' }]) {
      expect(() => sagaOutput('t', bad)).toThrow(SagaPayloadError);
    }
  });
});
