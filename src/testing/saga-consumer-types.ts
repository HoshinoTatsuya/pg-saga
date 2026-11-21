/**
 * Типовая проба: реалистичное использование публичного API так, как его пишет
 * потребитель, под `tsc --strict` (с `exactOptionalPropertyTypes`). Компилирует
 * `saga-context-types.spec.ts`: ни одной ошибки. Отрицательные случаи помечены
 * `@ts-expect-error` — если тип станет слишком широким (`any`), директива
 * останется без ошибки, и это тоже ошибка компиляции.
 */
import type { OutboxTransaction } from '@hoshinotatsuya/outbox';

import {
  defineSaga,
  type SagaAdmin,
  type SagaCommandMeta,
  type SagaContext,
  type SagaDefinition,
  type SagaOrchestrator,
  sagaReplyMessage,
  sagaReplyOf,
} from '../index';

// Вход — ИНТЕРФЕЙС (обычная форма DTO), а не type alias.
interface OrderInput {
  orderId: string;
  amount: number;
}
export const interfaceInput = defineSaga<OrderInput>('t1', { version: 1 })
  .step('a', { execute: async ({ context }) => ({ seen: `${context.orderId}:${context.amount}` }) })
  .step('b', {
    execute: async ({ context }) => {
      const seen: string = context.seen;
      // @ts-expect-error поля, которого шаги не дают, нет в контексте
      void context.missing;
      return { seen };
    },
  })
  .build();

// Выход шага — интерфейс и экземпляр класса (сущность из репозитория).
interface Reservation {
  reservationId: string;
}
class ChargeEntity {
  public constructor(public readonly chargeId: string) {}
}
const reserve = async (): Promise<Reservation> => ({ reservationId: 'r' });
export const interfaceOutput = defineSaga<{ orderId: string }>('t2', { version: 1 })
  .step('reserve', { execute: async () => reserve() })
  .step('charge', { execute: async () => new ChargeEntity('c') })
  .step('use', {
    execute: async ({ context }) => {
      const r: string = context.reservationId;
      const c: string = context.chargeId;
      return { both: r + c };
    },
  })
  .build();

// Локальная компенсация шага, который МЕНЯЕТ тип поля: контекст — после шага.
export const localRetype = defineSaga<{ status: 'new'; orderId: string }>('t3', { version: 1 })
  .step('pay', {
    execute: async () => ({ status: 'paid' as const }),
    compensate: async ({ context }) => {
      const status: 'paid' = context.status;
      const orderId: string = context.orderId;
      void status;
      void orderId;
    },
  })
  .build();

// Удалённая компенсация шага, меняющего тип поля: исход неизвестен — поле
// прежнего ИЛИ нового типа, новое поле — необязательно.
export const remoteRetype = defineSaga<{ status: 'new'; orderId: string }>('t4', { version: 1 })
  .remoteStep<{ status: 'charged'; chargeId: string }>('charge', {
    command: ({ context }) => ({ eventType: 'c', payload: context.orderId }),
    compensation: ({ context }) => {
      const orderId: string = context.orderId;
      const status: 'new' | 'charged' = context.status;
      const chargeId: string | undefined = context.chargeId;
      // @ts-expect-error выход шага мог не прийти: chargeId необязателен
      const sure: string = context.chargeId;
      void sure;
      return { eventType: 'r', payload: { orderId, status, chargeId: chargeId ?? null } };
    },
    timeoutMs: 1_000,
  })
  .step('after', {
    execute: async ({ context }) => {
      // После шага поля выхода — как пришли.
      const status: 'charged' = context.status;
      const chargeId: string = context.chargeId;
      return { done: `${status}:${chargeId}` };
    },
  })
  .build();

// start и register: вход проверяется, контекст после шагов не мешает.
declare const orchestrator: SagaOrchestrator;
declare const tx: OutboxTransaction;
export const startTyped = async (): Promise<void> => {
  orchestrator.register(interfaceOutput).register(localRetype).register(interfaceInput);
  await orchestrator.start(tx, interfaceOutput, { id: 'x', input: { orderId: 'o' } });
  await orchestrator.start(tx, interfaceInput, { id: 'x', input: { orderId: 'o', amount: 1 } });
  // @ts-expect-error вход чужой формы не компилируется
  await orchestrator.start(tx, interfaceOutput, { id: 'x', input: { wrong: 1 } });
};

// Обобщённая фабрика у потребителя: SagaDefinition<Input, Ctx> параметром.
export const runAny = async <I extends SagaContext>(definition: SagaDefinition<I, SagaContext>, input: I) =>
  orchestrator.start(tx, definition, { id: 'y', input });

// Шаг без выхода, затем шаг читает вход; сужение поля выходом шага.
export const narrowing = defineSaga<{ note?: string; amount: number | null }>('t8', { version: 1 })
  .step('noop', { execute: async () => {} })
  .step('fix', { execute: async () => ({ amount: 5 }) })
  .step('read', {
    execute: async ({ context }) => {
      const amount: number = context.amount;
      const note: string | undefined = context.note;
      return { r: `${amount}${note ?? ''}` };
    },
  })
  .build();

// Админка: retry с поправкой контекста и порогом простоя.
declare const admin: SagaAdmin;
export const adminCalls = async (): Promise<void> => {
  await admin.retry('id', 'note', { contextPatch: { amount: 100 } });
  await admin.retry('id', null, { stalledForMs: 30 * 24 * 60 * 60 * 1_000 });
};

// Вход — размеченное объединение: поля вариантов не теряются после шагов (и
// после шага без выхода, и после шага с выходом).
type KindInput = { kind: 'retail'; sku: string } | { kind: 'b2b'; contractId: number };
export const unionInput = defineSaga<KindInput>('t9', { version: 1 })
  .step('audit', { execute: async () => {} })
  .step('route', {
    execute: async ({ context }) =>
      context.kind === 'retail' ? { target: context.sku } : { target: String(context.contractId) },
  })
  .step('use', {
    execute: async ({ context }) => {
      const target: string = context.target;
      if (context.kind === 'b2b') {
        const contractId: number = context.contractId;
        // @ts-expect-error у варианта b2b нет sku
        void context.sku;
        return { out: `${target}:${contractId}` };
      }
      return { out: `${target}:${context.sku}` };
    },
  })
  .build();

// Ответ участника и поправка контекста — интерфейсы (DTO), а не Record.
interface ChargeDto {
  chargeId: string;
  amountCents: number;
}
interface AmountPatch {
  amount: number;
}
declare const meta: SagaCommandMeta;
declare const charge: ChargeDto;
declare const amountPatch: AmountPatch;
export const replyDto = sagaReplyMessage(sagaReplyOf(meta, { ok: true, data: charge }));
export const patchDto = async (): Promise<void> => {
  await admin.retry('id', 'n', { contextPatch: amountPatch });
  // @ts-expect-error поправка контекста — объект, не строка
  await admin.retry('id', 'n', { contextPatch: 'amount=1' });
};

// Выход шага — словарь: прежние поля контекста не стираются в типе.
export const dictionary = defineSaga<{ orderId: string }>('t10', { version: 1 })
  .step('prices', { execute: async (): Promise<Record<string, number>> => ({ a: 1 }) })
  .step('use', {
    execute: async ({ context }) => {
      const orderId: string = context.orderId;
      return { orderId };
    },
  })
  .build();
