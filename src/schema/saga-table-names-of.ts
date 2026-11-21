import { identifier } from '../config/config-identifier';
import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import type { SagaIndexNames } from '../types/saga-index-names.type';
import type { SagaTablesOptions } from '../types/saga-tables-options.type';

/**
 * Имена таблиц и имена ограничений и индексов: заданные (`indexNames`) или
 * производные от имени таблицы. Производное имя длиннее 63 байт — отказ с
 * подсказкой: Postgres обрезал бы его молча, и сверка схемы искала бы индекс,
 * которого под этим именем нет. Заданное — то же правило имени, что у таблиц;
 * неизвестный ключ `indexNames` — отказ (опечатка молча вернула бы
 * производное имя). Совпадение имён в одном пространстве — отказ здесь, а не
 * на середине миграции: таблицы, индексы и первичные ключи делят пространство
 * имён схемы, ограничения журнала — пространство таблицы.
 */
export const sagaTableNamesOf = (options: SagaTablesOptions) => {
  const instances = identifier('instances', options?.instances);
  const journal = identifier('journal', options?.journal);
  if (instances === journal) {
    throw new SagaConfigError('journal', 'совпадает с именем таблицы экземпляров');
  }
  const schema = options?.schema === undefined ? undefined : identifier('schema', options.schema);
  const derived: SagaIndexNames = {
    instancesPkey: `${instances}_pkey`,
    instancesStatusIdx: `${instances}_status_updated_idx`,
    instancesCreatedIdx: `${instances}_created_idx`,
    instancesMoveIdx: `${instances}_status_move_idx`,
    instancesStateCheck: `${instances}_state_check`,
    journalPkey: `${journal}_pkey`,
    journalSagaFkey: `${journal}_saga_id_fkey`,
    journalSagaIdx: `${journal}_saga_idx`,
    journalPhaseCheck: `${journal}_phase_check`,
  };
  const given = options?.indexNames ?? {};
  if (typeof given !== 'object' || given === null) {
    throw new SagaConfigError('indexNames', 'объект имён индексов и ограничений');
  }
  for (const key of Object.keys(given)) {
    if (!Object.hasOwn(derived, key)) {
      throw new SagaConfigError(
        `indexNames.${key}`,
        `неизвестное имя; есть ${Object.keys(derived).join(', ')}`,
      );
    }
  }
  const names = { ...derived };
  for (const key of Object.keys(derived) as Array<keyof SagaIndexNames>) {
    const value = given[key];
    if (value !== undefined) {
      names[key] = identifier(`indexNames.${key}`, value);
    } else if (Buffer.byteLength(derived[key]) > SAGA_LIMIT.identifierBytes) {
      throw new SagaConfigError(
        key,
        `производное имя длиннее ${SAGA_LIMIT.identifierBytes} байт: ${derived[key]} — задайте indexNames.${key}`,
      );
    }
  }
  const distinct = (fields: Readonly<Record<string, string>>): void => {
    const seen = new Map<string, string>();
    for (const [field, name] of Object.entries(fields)) {
      const first = seen.get(name);
      if (first !== undefined) {
        throw new SagaConfigError(field, `имя ${name} уже занято (${first})`);
      }
      seen.set(name, field);
    }
  };
  distinct({
    instances,
    journal,
    instancesPkey: names.instancesPkey,
    instancesStatusIdx: names.instancesStatusIdx,
    instancesCreatedIdx: names.instancesCreatedIdx,
    instancesMoveIdx: names.instancesMoveIdx,
    journalPkey: names.journalPkey,
    journalSagaIdx: names.journalSagaIdx,
  });
  distinct({ instancesPkey: names.instancesPkey, instancesStateCheck: names.instancesStateCheck });
  distinct({
    journalPkey: names.journalPkey,
    journalSagaFkey: names.journalSagaFkey,
    journalPhaseCheck: names.journalPhaseCheck,
  });
  return { instances, journal, ...names, schema };
};
