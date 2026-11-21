import { sagaStatementCeilingSql } from './saga-statement-ceiling-sql';

/**
 * Тень транзакции drizzle для шага: тот же объект (прототипом), но сессия — с
 * соединением, которое перед КАЖДЫМ запросом ставит `statement_timeout` в
 * остаток бюджета шага (`remaining`, мс; нет остатка — он бросает), если свой
 * потолок шага не короче (`sagaStatementCeilingSql`). Так потолок
 * получают `execute`, построители (`tx.insert(...)`), вложенная транзакция —
 * всё, что идёт через сессию транзакции. Реляционные запросы `tx.query.*`
 * держат сессию со своего построения: им остаётся потолок шага целиком.
 *
 * Без остатка-потолка запрос, начатый за миг до потолка шага, шёл бы на
 * сервере весь потолок: отмена ожидания в процессе запрос не останавливает,
 * ROLLBACK встаёт за ним, а запись отказа не дожидается строки экземпляра —
 * отказ не записан, шаг повторяется на каждой доставке до DEAD. Отмена с
 * другого соединения (`pg_cancel_backend`) не годится: под пулером в
 * transaction mode она может попасть в чужой запрос и требует свободного
 * соединения, которого под нагрузкой нет.
 *
 * Установка идёт в очередь соединения перед запросом (pg шлёт запросы одного
 * соединения по порядку): лишнего ожидания нет, а прерванная транзакция
 * откажет и самому запросу. Не drizzle (нет `session.client.query`) — `null`.
 */
export const sagaBudgetedTransaction = <T extends object>(
  tx: T,
  remaining: (property: PropertyKey) => number,
): T | null => {
  const session = (tx as { session?: unknown }).session as
    | { readonly client?: { readonly query?: unknown }; readonly prepareQuery?: unknown }
    | undefined;
  const client = session?.client;
  if (typeof session?.prepareQuery !== 'function' || typeof client?.query !== 'function') {
    return null;
  }
  const query = (client.query as (...args: unknown[]) => unknown).bind(client);
  const limited: unknown = Object.create(client, {
    query: {
      value: (...args: unknown[]): unknown => {
        const left = remaining('query');
        const settled = query(sagaStatementCeilingSql(left));
        (settled as { catch?: (onRejected: () => undefined) => unknown } | undefined)?.catch?.(
          () => undefined,
        );
        return query(...args);
      },
    },
  });
  const shadowSession: unknown = Object.create(session, { client: { value: limited } });
  return Object.create(tx, { session: { value: shadowSession } }) as T;
};
