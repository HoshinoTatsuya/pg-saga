import { sql } from 'drizzle-orm';

import { SagaStepTimeoutError } from '../errors/saga-step-timeout.error';
import type { SagaStepBudget } from '../types/saga-step-budget.type';
import { sagaBudgetedTransaction } from './saga-budgeted-transaction';
import { sagaStatementCeilingSql } from './saga-statement-ceiling-sql';

/**
 * Транзакция для кода локального шага, которая закрывается вместе с шагом:
 * после `close()` любое обращение к ней — отказ ДО обращения к соединению.
 *
 * Шаг, брошенный по таймауту (глухой к сигналу), продолжает исполняться: JS
 * не прерывает начатый вызов. Его транзакцию оркестратор к этому моменту
 * откатил, и соединение уже в пуле. Раннер `sagaTransactionRunner`
 * (`transactionKeepingFailure` pg-runtime) отказывает такому запросу на самом
 * соединении, а свой `deps.transaction` (например, `db.transaction` drizzle) —
 * нет: запрос ушёл бы мимо транзакции (автокоммитом) или в транзакцию
 * следующего владельца соединения, и эффект шага, записанный отказом, лёг бы в
 * базу — повтор записал бы его второй раз.
 *
 * Потолок запроса шага — ОСТАТОК бюджета шага (`budget`): перед каждым запросом
 * транзакции шага `statement_timeout` ставится в то, что осталось до потолка
 * шага (`sagaBudgetedTransaction`). Остатка нет — отказ таймаутом шага до
 * отправки.
 *
 * Ограда стоит на САМОМ объекте транзакции шага: объекты, полученные из него
 * до конца шага (вложенная транзакция, подготовленный запрос, построитель),
 * она не закрывает — их закрывает только защита соединения раннера. Методы
 * зовутся на самом объекте (или его тени с подменённой сессией), а не на
 * обёртке: приватные поля (`#x`) обёртку не видят.
 */
export const sagaFencedTransaction = <T extends object>(
  tx: T,
  step: string,
  budget: SagaStepBudget | null = null,
): { readonly tx: T; close(): void } => {
  let open = true;
  const closed = (property: PropertyKey): Error =>
    new Error(
      `pg-saga: шаг ${step} обратился к транзакции (${String(property)}) после своего конца — таймаута, ` +
        'отмены доставки или возврата: транзакция шага уже закрыта, соединение в пуле',
    );
  /** Остаток бюджета, мс, — или отказ: шаг закрыт либо бюджет исчерпан. */
  const remaining = (property: PropertyKey): number => {
    if (!open) {
      throw closed(property);
    }
    const left = budget === null ? 0 : Math.floor(budget.deadline - performance.now());
    if (budget !== null && left < 1) {
      throw new SagaStepTimeoutError(budget.limitMs);
    }
    return left;
  };
  const shadow = budget === null ? null : sagaBudgetedTransaction(tx, remaining);
  const target: T = shadow ?? tx;
  // Не drizzle: потолок — перед каждым `execute` (контракт транзакции outbox).
  const execute =
    budget !== null && shadow === null && typeof (tx as { execute?: unknown }).execute === 'function'
      ? async (...args: unknown[]): Promise<unknown> => {
          const run = (tx as unknown as { execute: (...args: unknown[]) => unknown }).execute.bind(tx);
          const left = remaining('execute');
          await run(sql.raw(sagaStatementCeilingSql(left)));
          if (!open) {
            throw closed('execute');
          }
          return await run(...args);
        }
      : null;
  const fenced = new Proxy(tx, {
    get(raw, property) {
      if (!open) {
        throw closed(property);
      }
      if (property === 'execute' && execute !== null) {
        return execute;
      }
      const value: unknown = Reflect.get(target, property, target);
      // Свойство самого экземпляра (подмена, шпион теста) — как есть; методы
      // прототипа — привязанными к объекту, а не к обёртке.
      if (typeof value !== 'function' || Object.hasOwn(raw, property)) {
        return value;
      }
      return (value as (...args: unknown[]) => unknown).bind(target);
    },
  });
  return {
    tx: fenced,
    close: () => {
      open = false;
    },
  };
};
