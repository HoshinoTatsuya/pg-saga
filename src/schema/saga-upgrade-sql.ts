import { OUTBOX_STATUS } from '@hoshinotatsuya/outbox';

import { identifier } from '../config/config-identifier';
import { sagaName } from '../config/config-name';
import { SAGA_AGGREGATE_TYPE } from '../constants/saga-aggregate-type.constant';
import { SAGA_EVENT_TYPE } from '../constants/saga-event-type.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaValueText } from '../runtime/saga-value-text';
import type { SagaUpgradeSqlOptions } from '../types/saga-upgrade-sql-options.type';
import { SAGA_CHECK_SQL } from './saga-check-sql';
import { SAGA_LAST_MOVE_SQL } from './saga-last-move-sql';
import { sagaTableNamesOf } from './saga-table-names-of';

/**
 * SQL доводки ЛЕТЯЩИХ данных версии 0.1.0 до 0.2.0 — выполнить ПОСЛЕ выкатки,
 * когда реплик 0.1.0 не осталось (они продолжали бы писать по-старому).
 * Идемпотентен; инструкции разделены `--> statement-breakpoint`. На объёме
 * выполняйте их ПО ОДНОЙ, каждую своей транзакцией (не одной миграцией): каждая
 * держит блокировки строк, которые меняет, до своего конца, а доставки саги
 * ждут строку экземпляра не дольше `lockTimeoutMs`.
 *
 * 1. `wake_at` активных экземпляров — заново по ждущим сообщениям outbox ровно
 *    их адреса (поколение, фаза, шаг, попытка): срок таймера ответа или паузы
 *    повтора; у немедленной команды — NULL, как пишет 0.2.0. Сообщение, которое
 *    уже доставлялось (`attempts > 0`), срока не знает — его `next_attempt_at`
 *    стал сроком повтора релея, — и `wake_at` такого адреса не меняется. 0.1.0 его не писала (ждущий её таймера экземпляр до срока
 *    выглядел бы замёрзшим в `stalledReport` и `stalledForMs`) и не сбрасывала
 *    (экземпляр, чей ответ применила реплика 0.1.0, держал бы срок таймера
 *    уже пройденного шага и до него не был бы виден замёрзшим).
 * 2. `wake_at` активного экземпляра, которого не ждёт ни одно сообщение его
 *    адреса, — сброшен (тот же второй случай: доставка следующего шага ушла в
 *    DEAD, а срок остался от прежнего).
 * 3. Агрегаты ждущих сообщений: 0.1.0 клала таймер, отложенную команду, ответ
 *    участника и команду участнику (по умолчанию) в агрегат экземпляра — при
 *    FIFO по агрегату они держали бы за собой команды саги, а команда участнику
 *    — команды другим участникам (`SAGA_AGGREGATE_TYPE`). Команда участнику без
 *    номера шага в служебной части (чужое событие) не трогается.
 *
 * Схема таблиц до 0.4.0 (можно до выкатки 0.4.0: реплики 0.3.0 пишут только
 * разрешённые значения):
 *
 * 4. CHECK экземпляров (статус, в том числе `blocked`; шаг, попытка, поколение)
 *    и журнала (фаза) — `NOT VALID`, затем `VALIDATE`: добавление не держит
 *    таблицу под исключительной блокировкой на время проверки строк, а
 *    проверка идёт под `SHARE UPDATE EXCLUSIVE` (запись не останавливает).
 *    Строка, нарушающая ограничение, — отказ `VALIDATE` (23514): исправьте её и
 *    повторите. Уже есть ограничение с этим именем — добавление пропускается.
 * 5. Индекс `(status, последнее движение, id)` для замёрзших и сторожа —
 *    `CREATE INDEX CONCURRENTLY IF NOT EXISTS`: ВНЕ транзакции (миграция в
 *    транзакции на нём откажет). Прерванная сборка оставляет индекс
 *    `INVALID` — удалите его (`DROP INDEX CONCURRENTLY`) и повторите.
 */
export const sagaUpgradeSql = (options: SagaUpgradeSqlOptions): string => {
  const schema = options?.schema === undefined ? undefined : identifier('schema', options.schema);
  const outboxSchema =
    options?.outboxSchema === undefined ? schema : identifier('outboxSchema', options.outboxSchema);
  const q = (name: string, inSchema: string | undefined): string =>
    inSchema === undefined ? `"${name}"` : `"${inSchema}"."${name}"`;
  const names = sagaTableNamesOf({
    instances: options?.instances,
    journal: options?.journal,
    ...(schema === undefined ? {} : { schema }),
    ...(options?.indexNames === undefined ? {} : { indexNames: options.indexNames }),
  } as never);
  const instances = q(identifier('instances', options?.instances), schema);
  const journal = q(names.journal, schema);
  const outbox = q(identifier('outbox', options?.outbox), outboxSchema);
  const literal = (value: string): string => `'${value.replaceAll("'", "''")}'`;
  const n = (name: string): string => `"${name}"`;
  const types = { ...SAGA_EVENT_TYPE, ...options?.eventTypes };
  for (const [kind, type] of Object.entries(types)) {
    sagaName(`eventTypes.${kind}`, type);
  }
  const zone = options?.outboxNaiveTimeZone;
  if (zone !== undefined && (typeof zone !== 'string' || !/^[A-Za-z0-9_+\-/:]{1,64}$/.test(zone))) {
    throw new SagaConfigError(
      'outboxNaiveTimeZone',
      `имя зоны (Europe/Moscow, UTC), получено ${sagaValueText(zone)}`,
    );
  }
  const due = zone === undefined ? 'o.next_attempt_at' : `(o.next_attempt_at AT TIME ZONE ${literal(zone)})`;
  const pending = literal(OUTBOX_STATUS.pending);
  const legacy = literal(SAGA_AGGREGATE_TYPE.command);
  const active = `(${literal(SAGA_STATUS.running)}, ${literal(SAGA_STATUS.compensating)})`;
  // Ждущие (и доставляемые сейчас) таймеры и команды исполнителю — по адресу.
  // Срок сообщение несёт, только пока его не доставляли (`attempts = 0`, иначе
  // `next_attempt_at` — срок повтора релея, а не саги): адрес с таким
  // сообщением (`known` ложно) `wake_at` не меняет — записала 0.2.0 — верный,
  // 0.1.0 не писала — NULL, простой считается от сдвига. Срок несёт сообщение,
  // записанное отложенным (`next_attempt_at` позже записи): таймер ответа и
  // пауза повтора; немедленной команде 0.2.0 срока не пишет (NULL).
  const carries = 'o.next_attempt_at > o.created_at';
  const awaited =
    'WITH m AS (\n' +
    "  SELECT o.payload->>'sagaId' AS saga_id, (o.payload->>'generation')::int AS generation,\n" +
    "         o.payload->>'phase' AS phase, (o.payload->>'stepIndex')::int AS step_index,\n" +
    `         (o.payload->>'attempt')::int AS attempt, max(${due}) FILTER (WHERE ${carries}) AS due,\n` +
    '         bool_and(o.attempts = 0) AS known\n' +
    `    FROM ${outbox} AS o\n` +
    `   WHERE o.status IN (${pending}, ${literal(OUTBOX_STATUS.dispatching)})\n` +
    `     AND o.event_type IN (${literal(types.timer)}, ${literal(types.command)})\n` +
    '   GROUP BY 1, 2, 3, 4, 5)\n';
  const sameAddress =
    'm.saga_id = i.id AND m.generation = i.generation AND m.step_index = i.step_index AND m.attempt = i.attempt\n' +
    `   AND m.phase = CASE WHEN i.status = ${literal(SAGA_STATUS.running)} THEN ${literal(SAGA_PHASE.execute)} ELSE ${literal(SAGA_PHASE.compensate)} END`;
  const ownId = (type: string, kind: string): string =>
    `UPDATE ${outbox} SET aggregate_type = ${literal(type)}, aggregate_id = id::text\n` +
    ` WHERE status = ${pending} AND aggregate_type = ${legacy} AND ${kind};`;
  return [
    `${awaited}UPDATE ${instances} AS i\n` +
      '   SET wake_at = m.due\n' +
      '  FROM m\n' +
      ` WHERE i.status IN ${active}\n` +
      `   AND ${sameAddress}\n` +
      '   AND m.known AND i.wake_at IS DISTINCT FROM m.due;',
    `${awaited}UPDATE ${instances} AS i\n` +
      '   SET wake_at = NULL\n' +
      ` WHERE i.status IN ${active} AND i.wake_at IS NOT NULL\n` +
      `   AND NOT EXISTS (SELECT 1 FROM m WHERE ${sameAddress});`,
    ownId(SAGA_AGGREGATE_TYPE.timer, `event_type = ${literal(types.timer)}`),
    ownId(
      SAGA_AGGREGATE_TYPE.deferred,
      `event_type = ${literal(types.command)} AND next_attempt_at > created_at`,
    ),
    ownId(SAGA_AGGREGATE_TYPE.reply, `event_type = ${literal(types.reply)}`),
    // Команда участнику — в агрегат своего шага (`sagaRemoteAggregateType`),
    // id агрегата остаётся id экземпляра.
    `UPDATE ${outbox} SET aggregate_type = ${literal(`${SAGA_AGGREGATE_TYPE.remote}:`)} || (payload->'sagaCommand'->>'stepIndex')\n` +
      ` WHERE status = ${pending} AND aggregate_type = ${legacy} AND payload->'sagaCommand' ? 'stepIndex'\n` +
      `   AND event_type NOT IN (${literal(types.command)}, ${literal(types.timer)}, ${literal(types.reply)});`,
    ...[
      [instances, n(names.instancesStateCheck), SAGA_CHECK_SQL.instancesState],
      [journal, n(names.journalPhaseCheck), SAGA_CHECK_SQL.journalPhase],
    ].flatMap(([table, constraint, expression]) => [
      'DO $saga$ BEGIN\n' +
        `  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = ${literal(table as string)}::regclass\n` +
        `                   AND conname = ${literal((constraint as string).slice(1, -1))}) THEN\n` +
        `    ALTER TABLE ${table} ADD CONSTRAINT ${constraint} CHECK (${expression}) NOT VALID;\n` +
        '  END IF;\nEND $saga$;',
      `ALTER TABLE ${table} VALIDATE CONSTRAINT ${constraint};`,
    ]),
    `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${n(names.instancesMoveIdx)} ON ${instances} USING btree ("status",(${SAGA_LAST_MOVE_SQL}),"id");`,
  ]
    .join('\n--> statement-breakpoint\n')
    .concat('\n');
};
