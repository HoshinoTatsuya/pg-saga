import { runInNewContext } from 'node:vm';

import { SagaAdmin } from '../admin/saga-admin';
import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { decodeTimestamp } from '../store/decode-timestamp';
import type { SagaOrchestratorDeps } from '../types/saga-orchestrator-deps.type';
import { sagaBackoffMs } from './saga-backoff-ms';
import { sagaEnqueueRejected } from './saga-enqueue-rejected';
import { sagaFencedTransaction } from './saga-fenced-transaction';
import { SagaOrchestrator } from './saga-orchestrator';
import { sagaOutput } from './saga-output';

/** База, к которой обращаться нельзя: проверка обязана отказать ДО запроса. */
const untouchable = {
  execute: async () => {
    throw new Error('запрос до проверки входа');
  },
  rollback: () => {
    throw new Error('rollback');
  },
};

const definition = defineSaga<{ n: number }>('inputs', { version: 1 })
  .step('a', { execute: async () => ({}) })
  .build();

const orchestrator = (): SagaOrchestrator =>
  new SagaOrchestrator({
    db: {},
    outbox: { enqueue: async () => undefined, config: { deliveryTimeoutMs: 20_000, coalescing: null } },
    inbox: { once: async () => ({ processed: false }) },
    tables: { instances: 'i', journal: 'j' },
    transaction: async () => {
      throw new Error('транзакций в этой спеке нет');
    },
  } as unknown as SagaOrchestratorDeps).register(definition);

const admin = (): SagaAdmin =>
  new SagaAdmin({
    db: untouchable,
    transaction: async () => {
      throw new Error('транзакция до проверки входа');
    },
    outbox: { enqueue: async () => undefined } as never,
    tables: { instances: 'i', journal: 'j' },
    commandEventType: 'saga.command',
  });

const META = {
  commandId: '00000000-0000-4000-8000-000000000000',
  sagaId: 's',
  saga: 'order',
  step: 'a',
  generation: 0,
  phase: 'execute',
  stepIndex: 0,
  attempt: 1,
  stepKey: 's:execute:0',
};

describe('вход вызывающего — отказ с именем поля до записи (S1.4, S1.6)', () => {
  it.each([
    ['id с NUL', { id: 'a\u0000b', input: { n: 1 } }, undefined],
    ['id с непарным суррогатом', { id: 'a\ud800', input: { n: 1 } }, undefined],
    ['вход null', { id: 'a', input: null }, undefined],
    ['вход — массив', { id: 'a', input: [1] }, undefined],
    ['момент старта — Invalid Date', { id: 'a', input: { n: 1 } }, new Date(Number.NaN)],
  ] as const)('start: %s — SagaConfigError', async (_name, args, now) => {
    await expect(
      orchestrator().start(untouchable as never, definition, args as never, now as never),
    ).rejects.toBeInstanceOf(SagaConfigError);
  });

  it('cancel: момент отмены — Invalid Date или не Date — SagaConfigError до блокировки строки', async () => {
    for (const at of [new Date(Number.NaN), '2026-11-01' as never]) {
      await expect(orchestrator().cancel(untouchable as never, 'a', 'причина', at)).rejects.toBeInstanceOf(
        SagaConfigError,
      );
    }
  });

  it('SagaAdmin: prune с Invalid Date и сверх потолка пачки, list с Invalid Date, пустая поправка — отказ до запроса', async () => {
    await expect(admin().prune(new Date(Number.NaN))).rejects.toBeInstanceOf(SagaConfigError);
    await expect(admin().prune(new Date(), 10_001)).rejects.toBeInstanceOf(SagaConfigError);
    await expect(admin().list({ updatedBefore: new Date(Number.NaN) })).rejects.toBeInstanceOf(
      SagaConfigError,
    );
    for (const [contextPatch, kind] of [
      [{}, SagaConfigError],
      [null, SagaConfigError],
      [[1], SagaPayloadError],
      ['x', SagaPayloadError],
    ] as const) {
      await expect(admin().retry('a', null, { contextPatch: contextPatch as never })).rejects.toBeInstanceOf(
        kind,
      );
    }
  });

  it('Date из другого контекста (vm) принимается как момент', async () => {
    const foreign = runInNewContext('new Date("2026-11-01T00:00:00Z")') as Date;
    expect(foreign instanceof Date).toBe(false);
    // Отказ — уже от базы (её трогать нельзя), а не проверки момента.
    await expect(admin().prune(foreign)).rejects.toThrow('запрос до проверки входа');
  });
});

describe('мелкие пробелы проверок', () => {
  it('S3.1: RegExp, буферы, типизированные массивы, WeakRef — отказ шага, а не молчаливый {} или ключи «0»…', () => {
    for (const bad of [
      { re: /x/ },
      { buffer: new ArrayBuffer(2) },
      { shared: new SharedArrayBuffer(2) },
      { bytes: new Uint8Array([1, 2]) },
      { view: new DataView(new ArrayBuffer(2)) },
      { ref: new WeakRef({}) },
    ]) {
      expect(() => sagaOutput('шаг', bad)).toThrow(SagaPayloadError);
    }
    // Двойник: Buffer несёт свой toJSON ({ type, data }) — данные не теряются.
    expect(sagaOutput('шаг', { b: Buffer.from([1]) })).toEqual({ b: { type: 'Buffer', data: [1] } });
  });

  it('S6.3: повтор имени шага в определении мимо построителя — отказ register', () => {
    const [step] = definition.steps;
    const twice = { ...definition, name: 'twice', steps: [step, step] };
    expect(() => orchestrator().register(twice as never)).toThrow(/имя шага повторяется/);
  });

  it('S6.7: источник случайности отдал NaN или Infinity — пауза из середины, а не NaN', () => {
    const retry = { maxAttempts: 3, backoffMs: 100, maxBackoffMs: 1_000 };
    expect(
      [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY].map((v) =>
        sagaBackoffMs(retry, 1, () => v),
      ),
    ).toEqual([75, 75, 75]);
  });

  it('S6.9: версия саги в команде участнику сверх int4 — отказ разбора', () => {
    expect(() => parseSagaCommand({ sagaCommand: { ...META, version: 2 ** 31 }, data: {} })).toThrow(
      SagaPayloadError,
    );
    expect(() =>
      parseSagaCommand({ sagaCommand: { ...META, version: 2 ** 31 - 1 }, data: {} }),
    ).not.toThrow();
    expect(() => parseSagaCommand([{ sagaCommand: META }])).toThrow(/нет sagaCommand/);
  });

  it('S6.5: проверка события outbox, завёрнутая в cause, и из другого контекста — детерминированный отказ', () => {
    const validation = new RangeError('outbox: payload больше maxPayloadBytes');
    expect(sagaEnqueueRejected(new Error('своя обёртка', { cause: validation }))).toBe(true);
    const foreign = runInNewContext('new TypeError("outbox.enqueue: NUL в payload")') as unknown;
    expect(foreign instanceof TypeError).toBe(false);
    expect(sagaEnqueueRejected(foreign)).toBe(true);
    // Двойник: рядом отказ базы — повторит доставка.
    const withDatabase = Object.assign(new Error('отказ базы', { cause: validation }), {
      name: 'OutboxDatabaseError',
    });
    expect(sagaEnqueueRejected(withDatabase)).toBe(false);
    expect(sagaEnqueueRejected(new Error('обёртка', { cause: new TypeError('Cannot read x') }))).toBe(false);
  });

  it('S6.4: транзакция с приватными полями (#x) — геттеры и методы работают через ограду', async () => {
    class OwnTransaction {
      #rows = ['r'];

      get size(): number {
        return this.#rows.length;
      }

      async execute(): Promise<{ rows: string[] }> {
        return { rows: this.#rows };
      }

      rollback(): never {
        throw new Error('rollback');
      }
    }
    const fence = sagaFencedTransaction(new OwnTransaction(), 'a');
    expect(fence.tx.size).toBe(1);
    expect(await fence.tx.execute()).toEqual({ rows: ['r'] });
    fence.close();
    expect(() => fence.tx.size).toThrow(/после своего конца/);
  });

  it('S3.3: Invalid Date из другого контекста — понятный отказ; верная дата оттуда — разбирается', () => {
    const invalid = runInNewContext('new Date(NaN)') as unknown;
    expect(() => decodeTimestamp(invalid)).toThrow(/время из базы не разобрано: Invalid Date/);
    const valid = runInNewContext('new Date("2026-11-01T12:00:00.000Z")') as unknown;
    expect(decodeTimestamp(valid).toISOString()).toBe('2026-11-01T12:00:00.000Z');
  });
});
