import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';

import { OutboxStore } from '@hoshinotatsuya/outbox';
import { sql } from 'drizzle-orm';

import { SagaAdmin } from '../admin/saga-admin';
import { identifier } from '../config/config-identifier';
import { sagaName } from '../config/config-name';
import { SAGA_DEFAULTS } from '../constants/saga-defaults.constant';
import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { sagaErrorText } from '../runtime/saga-error-text';
import { sagaOutput } from '../runtime/saga-output';
import { sagaValueText } from '../runtime/saga-value-text';
import type { SagaContext } from '../types/saga-context.type';
import type { SagaStuckCliIo } from './saga-stuck-cli-io.type';

/**
 * CLI ручного разбора застрявших саг:
 * - `list [--stalled-ms N]` — отчёт о застрявших (старейшие первыми); с `--stalled-ms` —
 *   о замёрзших активных: не двигались не меньше N мс (доставка ушла в DEAD);
 * - `show --id` — экземпляр и журнал;
 * - `retry --id [--note] [--context-patch <json> | --context-patch-file <путь>] --outbox --command-type` —
 *   повторить компенсацию с того же шага; поправка контекста — поля поверх прежних
 *   (`SagaAdmin.retry`, `contextPatch`): застряли на неверных бизнес-данных — повтор
 *   без поправки упадёт так же. Из файла — когда данные не должны попасть в список
 *   процессов;
 * - `resolve --id --note` — пометить разобранным;
 * - `stats [--since-ms N]` — срез по сагам за окно (по умолчанию сутки);
 * - `prune --older-than-ms N [--limit] --yes` — удалить завершённые старше N мс.
 * `retry`/`resolve` с `--stalled-ms N` — и над замёрзшим активным (не двигался ≥ N мс).
 * `--help` — справка.
 *
 * `retry` и `resolve` без `--yes` ничего не меняют: печатают экземпляр и что
 * будет сделано. Адрес базы — из `SAGA_DATABASE_URL` или `DATABASE_URL`, не
 * аргументом: аргументы видны в списке процессов.
 *
 * Код выхода: 0 — сделано, 1 — отказ базы или состояния, 2 — неверный вызов.
 */
export const runSagaStuckCli = async (argv: readonly string[], io: SagaStuckCliIo): Promise<number> => {
  const USAGE =
    'saga-stuck <list|show|retry|resolve|stats|prune> --instances <таблица> --journal <таблица> ' +
    '[--outbox <таблица>] [--schema <схема>] [--command-type <тип>] [--id <id>] [--note <причина>] ' +
    '[--context-patch <json-объект> | --context-patch-file <путь>] ' +
    '[--limit <n>] [--stalled-ms <мс>] [--since-ms <мс>] [--older-than-ms <мс>] [--yes]';
  const COMMANDS = ['list', 'show', 'retry', 'resolve', 'stats', 'prune'];
  let parsed: ReturnType<typeof parseArgs>;
  try {
    parsed = parseArgs({
      args: [...argv],
      allowPositionals: true,
      strict: true,
      options: {
        instances: { type: 'string' },
        journal: { type: 'string' },
        outbox: { type: 'string' },
        schema: { type: 'string' },
        'command-type': { type: 'string' },
        id: { type: 'string' },
        note: { type: 'string' },
        'context-patch': { type: 'string' },
        'context-patch-file': { type: 'string' },
        limit: { type: 'string' },
        'stalled-ms': { type: 'string' },
        'since-ms': { type: 'string' },
        'older-than-ms': { type: 'string' },
        yes: { type: 'boolean' },
        help: { type: 'boolean', short: 'h' },
      },
    });
  } catch (error) {
    io.stderr(
      `saga-stuck: ${sagaValueText((error as { message?: unknown } | null)?.message ?? error)}\n${USAGE}`,
    );
    return 2;
  }
  const { values, positionals } = parsed;
  const [command] = positionals;
  const str = (name: string): string | undefined =>
    typeof values[name] === 'string' ? (values[name] as string) : undefined;
  if (values.help === true) {
    io.stdout(USAGE);
    return 0;
  }
  if (positionals.length !== 1 || !COMMANDS.includes(command ?? '')) {
    io.stderr(`saga-stuck: команда — одна из ${COMMANDS.join(', ')}\n${USAGE}`);
    return 2;
  }
  /**
   * Целое ≥ 1 строго цифрами: `Number('1e3')` и `Number('0x10')` — тоже целые,
   * а оператор, набравший их, имел в виду не то. Потолок — у каждого значения свой.
   */
  const count = (name: string, fallback: number | undefined, max: number): number | undefined | null => {
    const text = str(name);
    if (text === undefined) {
      return fallback;
    }
    const value = /^[1-9][0-9]*$/.test(text) ? Number(text) : Number.NaN;
    if (!Number.isSafeInteger(value) || value > max) {
      io.stderr(`saga-stuck: --${name} — целое от 1 до ${max} цифрами, получено ${text}`);
      return null;
    }
    return value;
  };
  const instances = str('instances');
  const journal = str('journal');
  if (instances === undefined || journal === undefined) {
    io.stderr(`saga-stuck: нужны --instances и --journal\n${USAGE}`);
    return 2;
  }
  const id = str('id');
  if (['show', 'retry', 'resolve'].includes(command ?? '') && (id === undefined || id.length === 0)) {
    io.stderr(`saga-stuck: ${String(command)} — нужен непустой --id`);
    return 2;
  }
  // Имена таблиц, схема и тип команды — до подключения: неверное имя — неверный
  // вызов (код 2), а не отказ базы (код 1).
  try {
    identifier('instances', instances);
    identifier('journal', journal);
    for (const name of ['outbox', 'schema'] as const) {
      if (str(name) !== undefined) {
        identifier(name, str(name));
      }
    }
    if (str('command-type') !== undefined) {
      sagaName('command-type', str('command-type'));
    }
  } catch (error) {
    io.stderr(`saga-stuck: ${sagaErrorText(error)}`);
    return 2;
  }
  const note = str('note');
  if (command === 'resolve' && (note === undefined || note.trim().length === 0)) {
    io.stderr('saga-stuck: resolve — нужна причина --note');
    return 2;
  }
  const outboxTable = str('outbox');
  if (command === 'retry' && outboxTable === undefined) {
    io.stderr('saga-stuck: retry — нужен --outbox (команда компенсации пишется в него)');
    return 2;
  }
  if (command === 'retry' && str('command-type') === undefined) {
    // Угаданный тип по умолчанию при своих eventTypes оркестратора уходит в
    // DEAD, а экземпляр навсегда остаётся compensating.
    io.stderr('saga-stuck: retry — нужен --command-type (тип команды оркестратора, eventTypes.command)');
    return 2;
  }
  // Поправка контекста — до подключения: неверная поправка — неверный вызов (код 2).
  const patchText = str('context-patch');
  const patchFile = str('context-patch-file');
  let contextPatch: SagaContext | undefined;
  if (patchText !== undefined || patchFile !== undefined) {
    if (command !== 'retry') {
      io.stderr('saga-stuck: --context-patch и --context-patch-file — только у retry');
      return 2;
    }
    if (patchText !== undefined && patchFile !== undefined) {
      io.stderr('saga-stuck: --context-patch или --context-patch-file — одно из двух');
      return 2;
    }
    let source: string;
    try {
      source = patchText ?? (await (io.readFile ?? ((path) => readFile(path, 'utf8')))(patchFile as string));
    } catch (error) {
      io.stderr(`saga-stuck: --context-patch-file не прочитан: ${sagaErrorText(error)}`);
      return 2;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(source);
    } catch (error) {
      io.stderr(`saga-stuck: поправка контекста — не JSON: ${sagaErrorText(error)}`);
      return 2;
    }
    // Объект с полями: массив, примитив, null и пустой объект поправкой не являются.
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      Array.isArray(parsed) ||
      Object.keys(parsed).length === 0
    ) {
      io.stderr('saga-stuck: поправка контекста — JSON-объект хотя бы с одним полем');
      return 2;
    }
    try {
      contextPatch = sagaOutput('поправка контекста', parsed);
    } catch (error) {
      io.stderr(`saga-stuck: ${sagaErrorText(error)}`);
      return 2;
    }
  }
  // Потолок длительностей — тот же, что у API (`durationMs`): свой здесь
  // разошёлся бы с ним (CLI принимал бы срок, на котором отказывает админка).
  const limit = count('limit', command === 'prune' ? 5_000 : 100, SAGA_LIMIT.listLimit);
  const stalledForMs = count('stalled-ms', undefined, SAGA_LIMIT.durationMaxMs);
  const sinceMs = count('since-ms', 24 * 60 * 60 * 1000, SAGA_LIMIT.durationMaxMs);
  const olderThanMs = count('older-than-ms', undefined, SAGA_LIMIT.durationMaxMs);
  if (
    limit === null ||
    limit === undefined ||
    stalledForMs === null ||
    sinceMs === null ||
    sinceMs === undefined ||
    olderThanMs === null
  ) {
    return 2;
  }
  if (command === 'prune' && olderThanMs === undefined) {
    io.stderr('saga-stuck: prune — нужен --older-than-ms (удаляются завершённые старше него)');
    return 2;
  }
  const stalled = stalledForMs === undefined ? {} : { stalledForMs };
  const url = io.env.SAGA_DATABASE_URL ?? io.env.DATABASE_URL;
  if (url === undefined || url.length === 0) {
    io.stderr('saga-stuck: адрес базы — в SAGA_DATABASE_URL или DATABASE_URL');
    return 2;
  }
  const schema = str('schema');
  const tables = { instances, journal, ...(schema === undefined ? {} : { schema }) };
  let connection: Awaited<ReturnType<SagaStuckCliIo['connect']>>;
  try {
    connection = await io.connect(url);
  } catch (error) {
    io.stderr(`saga-stuck: подключение не удалось: ${sagaErrorText(error)}`);
    return 1;
  }
  try {
    const commandType = str('command-type');
    // Outbox нужен только retry: прочим командам несуществующая таблица-заглушка
    // не подставляется — её отсутствие не должно ничего значить.
    const admin = new SagaAdmin({
      db: connection.executor,
      // Потолки транзакции разбора — как у раннера саги: retry/resolve ждут
      // строку экземпляра не дольше lockTimeoutMs (а не висят за идущим
      // шагом), а сессия, брошенная в замёрзшей сети, не держит строку дольше
      // потолка простоя.
      transaction: (work) =>
        connection.transaction(async (tx) => {
          await tx.execute(
            sql`SELECT set_config('lock_timeout', ${`${SAGA_DEFAULTS.lockTimeoutMs}ms`}, true),
                       set_config('idle_in_transaction_session_timeout', ${`${SAGA_DEFAULTS.idleInTransactionTimeoutMs}ms`}, true)`,
          );
          return await work(tx);
        }),
      ...(outboxTable === undefined
        ? {}
        : {
            outbox: new OutboxStore(connection.executor, {
              table: outboxTable,
              ...(schema === undefined ? {} : { schema }),
            }),
          }),
      tables,
      ...(commandType === undefined ? {} : { commandEventType: commandType }),
    });
    if (command === 'stats') {
      for (const row of await admin.stats(new Date(Date.now() - sinceMs))) {
        io.stdout(JSON.stringify(row));
      }
      return 0;
    }
    if (command === 'prune') {
      const before = new Date(Date.now() - (olderThanMs as number));
      if (values.yes !== true) {
        io.stdout(
          `prune: ничего не удалено — добавьте --yes (удалятся завершённые до ${before.toISOString()})`,
        );
        return 0;
      }
      io.stdout(`prune: удалено ${await admin.prune(before, limit)}`);
      return 0;
    }
    if (command === 'list') {
      const report =
        stalledForMs === undefined
          ? await admin.stuckReport(limit)
          : await admin.stalledReport(stalledForMs, limit);
      for (const item of report) {
        io.stdout(JSON.stringify(item));
      }
      io.stdout(
        `${stalledForMs === undefined ? 'застрявших' : 'замёрзших'}: ${report.length}${
          report.length === limit ? ' (упёрлись в --limit)' : ''
        }`,
      );
      return 0;
    }
    const shown = await admin.show(id as string);
    if (shown === null) {
      io.stderr(`saga-stuck: экземпляра ${String(id)} нет`);
      return 1;
    }
    if (command !== 'show' && !SagaAdmin.eligible(shown.instance, new Date(), stalledForMs)) {
      // Без --yes тоже: не обещать «добавьте --yes» тому, на чём --yes откажет.
      io.stderr(
        `saga-stuck: ${String(command)}: экземпляр ${String(id)} в состоянии ${shown.instance.status} — ` +
          'нужен stuck, blocked или замёрзший (--stalled-ms)',
      );
      return 1;
    }
    if (command === 'show' || values.yes !== true) {
      io.stdout(JSON.stringify(shown.instance));
      for (const entry of shown.journal) {
        io.stdout(JSON.stringify(entry));
      }
      if (contextPatch !== undefined) {
        // Ключи, а не значения: значения — бизнес-данные, им не место в выводе.
        io.stdout(`${String(command)}: поправка контекста — поля ${Object.keys(contextPatch).join(', ')}`);
      }
      if (command !== 'show') {
        io.stdout(`${String(command)}: ничего не изменено — добавьте --yes`);
      }
      return 0;
    }
    if (command === 'retry') {
      await admin.retry(id as string, note ?? null, {
        ...stalled,
        ...(contextPatch === undefined ? {} : { contextPatch }),
      });
      io.stdout(`retry: шаг экземпляра ${String(id)} поставлен заново`);
    } else {
      await admin.resolve(id as string, note as string, stalled);
      io.stdout(`resolve: экземпляр ${String(id)} помечен разобранным`);
    }
    return 0;
  } catch (error) {
    io.stderr(`saga-stuck: ${sagaErrorText(error)}`);
    return 1;
  } finally {
    await connection.close().catch(() => undefined);
  }
};
