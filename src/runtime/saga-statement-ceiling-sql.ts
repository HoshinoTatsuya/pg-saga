/**
 * Запрос, ставящий `statement_timeout` транзакции (is_local) в остаток бюджета
 * шага `leftMs` — или оставляющий свой потолок шага, если тот меньше: шаг,
 * сам поставивший короткий потолок своему запросу, его не теряет (отмена
 * сервером, 57014, — таймаут шага). `0` у настройки — «без потолка», то есть
 * больше любого остатка. Число — целое мс: текст запроса без параметров
 * (простой протокол, одна посылка).
 */
export const sagaStatementCeilingSql = (leftMs: number): string => {
  const left = String(Math.max(1, Math.floor(leftMs)));
  return (
    "SELECT set_config('statement_timeout', (CASE WHEN own > 0 AND own < " +
    `${left} THEN own ELSE ${left} END)::text || 'ms', true) ` +
    "FROM (SELECT (extract(epoch FROM current_setting('statement_timeout')::interval) * 1000)::bigint AS own) AS ceiling"
  );
};
