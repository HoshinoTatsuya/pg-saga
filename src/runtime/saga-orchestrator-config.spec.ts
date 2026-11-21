import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaDefinitionUnavailableError } from '../errors/saga-definition-unavailable.error';
import type { SagaOrchestratorDeps } from '../types/saga-orchestrator-deps.type';
import { SagaOrchestrator } from './saga-orchestrator';
import { sagaTransactionRunner } from './saga-transaction-runner';

const noop = async (): Promise<void> => undefined;

/** Настройка без базы: зависимости — заглушки, транзакций здесь не бывает. */
const orchestrator = (
  config: Record<string, unknown> | null = { deliveryTimeoutMs: 20_000, coalescing: null },
  extra: Partial<SagaOrchestratorDeps> = {},
): SagaOrchestrator =>
  new SagaOrchestrator({
    db: {},
    outbox: { enqueue: async () => undefined, ...(config === null ? {} : { config }) },
    inbox: { once: async () => ({ processed: false }) },
    tables: { instances: 'i', journal: 'j' },
    transaction: async () => {
      throw new Error('транзакций в этой спеке нет');
    },
    ...extra,
  } as unknown as SagaOrchestratorDeps);

const sagaWith = (timeoutMs?: number) =>
  defineSaga<{ id: string }>('slow', { version: 1 })
    .step('a', { execute: noop, ...(timeoutMs === undefined ? {} : { timeoutMs }) })
    .build();

const remoteSaga = () =>
  defineSaga<{ id: string }>('remote', { version: 1 })
    .remoteStep('charge', { command: () => ({ eventType: 'billing.charge', payload: {} }), timeoutMs: 1_000 })
    .build();

describe('SagaOrchestrator.register: определение мимо построителя', () => {
  it('пустая сага и имя шага длиннее 64 знаков — отказ на старте, а не посреди перехода', () => {
    expect(() => orchestrator().register({ name: 'raw', version: 1, steps: [] } as never)).toThrow(
      SagaConfigError,
    );
    const longStep = { ...sagaWith(), steps: [{ ...sagaWith().steps[0], name: 'x'.repeat(65) }] };
    expect(() => orchestrator().register(longStep as never)).toThrow(SagaConfigError);
  });

  it.each([
    ['удалённый шаг без command', { command: null }],
    ['удалённый шаг без timeoutMs', { timeoutMs: null }],
    ['удалённый шаг с timeoutMs 0', { timeoutMs: 0 }],
    ['удалённый шаг с локальной compensate', { compensate: noop }],
    ['вид шага неизвестен', { kind: 'other' }],
    ['повтор без maxAttempts', { retry: { backoffMs: 1, maxBackoffMs: 1 } }],
    [
      'крышка паузы компенсации меньше базы',
      { compensationRetry: { maxAttempts: 1, backoffMs: 10, maxBackoffMs: 1 } },
    ],
  ])('%s — отказ register, а не отказ посреди доставки до DEAD', (_name, patch) => {
    const base = remoteSaga();
    const broken = { ...base, steps: [{ ...base.steps[0], ...patch }] };
    expect(() => orchestrator().register(broken as never)).toThrow(SagaConfigError);
  });

  it('двойники: определение построителя (локальное и удалённое) принимается', () => {
    expect(() => orchestrator().register(remoteSaga()).register(sagaWith())).not.toThrow();
    const local = sagaWith();
    expect(() =>
      orchestrator().register({ ...local, steps: [{ ...local.steps[0], timeoutMs: null }] } as never),
    ).not.toThrow();
    const missing = { ...local, steps: [{ ...local.steps[0], execute: null }] };
    expect(() => orchestrator().register(missing as never)).toThrow(SagaConfigError);
  });
});

describe('SagaOrchestrator: потолок доставки релея — из настроек хранилища outbox', () => {
  it('шаг вместе с ожиданием строки (5 с) и ROLLBACK (2 с) не короче потолка outbox — отказ register', () => {
    expect(() => orchestrator({ deliveryTimeoutMs: 20_000 }).register(sagaWith(13_000))).toThrow(
      /ожидание строки 5000 мс \+ ROLLBACK 2000 мс не меньше потолка доставки релея 20000/,
    );
    expect(() => orchestrator({ deliveryTimeoutMs: 20_000 }).register(sagaWith(12_999))).not.toThrow();
    // Шаг по умолчанию укладывается в потолок outbox по умолчанию.
    expect(() => orchestrator({ deliveryTimeoutMs: 20_000 }).register(sagaWith())).not.toThrow();
  });

  it('ожидание строки и ROLLBACK — те, что применяет РАННЕР, а не настройки оркестратора', () => {
    const fakeDb = { $client: {} };
    const runner = (options: Parameters<typeof sagaTransactionRunner>[1]) =>
      orchestrator({ deliveryTimeoutMs: 20_000 }, { transaction: sagaTransactionRunner(fakeDb, options) });
    // Раннер ждёт строку 15 с: шаг 12 с не уложится, хотя у оркестратора ожидание по умолчанию 5 с.
    expect(() => runner({ lockTimeoutMs: 15_000 }).register(sagaWith())).toThrow(/ожидание строки 15000 мс/);
    expect(() => runner({ rollbackTimeoutMs: 4_000 }).register(sagaWith())).toThrow(/ROLLBACK 4000 мс/);
    // Двойник: раннер с короткими потолками принимает шаг, который отверг бы оркестратор с deps.lockTimeoutMs 15 с.
    expect(() =>
      orchestrator(
        { deliveryTimeoutMs: 20_000 },
        { transaction: sagaTransactionRunner(fakeDb, { lockTimeoutMs: 1_000 }), lockTimeoutMs: 15_000 },
      ).register(sagaWith(15_000)),
    ).not.toThrow();
  });

  it.each([Number.NaN, -1, 0, 1.5, Number.POSITIVE_INFINITY])(
    'lockTimeoutMs оркестратора %p — отказ в конструкторе, а не замерзание на первой доставке',
    (lockTimeoutMs) => {
      expect(() => orchestrator(undefined, { lockTimeoutMs })).toThrow(SagaConfigError);
    },
  );

  it.each([
    ['lockTimeoutMs', Number.NaN],
    ['rollbackTimeoutMs', 0],
    ['attempts', 1.5],
  ] as const)('sagaTransactionRunner: %s = %p — SagaConfigError', (name, value) => {
    expect(() => sagaTransactionRunner({ $client: {} }, { [name]: value })).toThrow(SagaConfigError);
  });

  it('потолок outbox меньше значения релея по умолчанию (10 с): шаг 3 с + 5 с + 2 с — отказ; потолок больше — шаг в минуту принят', () => {
    expect(() => orchestrator({ deliveryTimeoutMs: 10_000 }).register(sagaWith(3_000))).toThrow(
      /не меньше потолка доставки релея 10000/,
    );
    // Шаг по умолчанию (12 с) при потолке 10 с — тоже отказ: сверка не отключается.
    expect(() => orchestrator({ deliveryTimeoutMs: 10_000 }).register(sagaWith())).toThrow(SagaConfigError);
    expect(() => orchestrator({ deliveryTimeoutMs: 120_000 }).register(sagaWith(60_000))).not.toThrow();
  });

  it('outbox без config (не хранилище OutboxStore) — отказ на старте, а не сверка с выдуманным потолком', () => {
    expect(() => orchestrator(null)).toThrow(/outbox\.config\.deliveryTimeoutMs/);
  });

  it('registerHandlers подписывает три типа событий саги', () => {
    const registered: string[] = [];
    orchestrator().registerHandlers({ register: (type: string) => registered.push(type) });
    expect(registered.sort()).toEqual(['saga.command', 'saga.reply', 'saga.timer']);
  });
});

describe('SagaOrchestrator: типы событий саги и outbox', () => {
  it('тип длиннее varchar(64) или с правилом слияния в outbox — отказ на старте', () => {
    expect(() =>
      orchestrator(
        { deliveryTimeoutMs: 20_000 },
        {
          eventTypes: { timer: `t.${'x'.repeat(70)}` },
        },
      ),
    ).toThrow(SagaConfigError);
    expect(() =>
      orchestrator({ deliveryTimeoutMs: 20_000, coalescing: { rules: { 'saga.reply': {} } } }),
    ).toThrow(/правило слияния/);
    // Двойник: правило слияния у ДРУГОГО типа — не помеха.
    expect(() =>
      orchestrator({ deliveryTimeoutMs: 20_000, coalescing: { rules: { 'orders.changed': {} } } }),
    ).not.toThrow();
  });
});

describe('SagaOrchestrator: проверки конфигурации', () => {
  it('типы команды, таймера и ответа совпадают — отказ на старте; разные свои — приняты', () => {
    expect(() => orchestrator(undefined, { eventTypes: { timer: 'saga.command' } })).toThrow(/различаться/);
    expect(() => orchestrator(undefined, { eventTypes: { reply: 'saga.timer' } })).toThrow(/различаться/);
    expect(() =>
      orchestrator(undefined, { eventTypes: { command: 'o.cmd', timer: 'o.timer', reply: 'o.reply' } }),
    ).not.toThrow();
  });

  it('та же версия саги второй раз — отказ register (молчаливой подмены нет); другая версия — принята', () => {
    const first = sagaWith();
    const target = orchestrator().register(first);
    expect(() => target.register(first)).toThrow(/уже зарегистрирована/);
    expect(() => target.register(sagaWith())).toThrow(/уже зарегистрирована/);
    expect(() =>
      target.register(
        defineSaga<{ id: string }>('slow', { version: 2 }).step('a', { execute: noop }).build(),
      ),
    ).not.toThrow();
  });

  it('хук definitionUnavailable — только на отказ «нет версии/шаги разошлись», не на любой отказ доставки', async () => {
    const events: string[] = [];
    const failing = (error: Error) =>
      orchestrator(undefined, {
        transaction: async () => {
          throw error;
        },
        metrics: { definitionUnavailable: (event) => events.push(`${event.saga}@${event.version}`) },
      });
    const command = {
      commandId: '00000000-0000-4000-8000-000000000000',
      sagaId: 's',
      generation: 0,
      phase: 'execute',
      stepIndex: 0,
      attempt: 1,
    };
    await expect(failing(new Error('обрыв соединения')).handle('saga.command', command)).rejects.toThrow(
      'обрыв соединения',
    );
    expect(events).toEqual([]);
    // Двойник: настоящий отказ определения — хук вызван.
    await expect(
      failing(new SagaDefinitionUnavailableError('order', 3, 'версия не зарегистрирована')).handle(
        'saga.command',
        command,
      ),
    ).rejects.toThrow(SagaDefinitionUnavailableError);
    expect(events).toEqual(['order@3']);
  });
});
