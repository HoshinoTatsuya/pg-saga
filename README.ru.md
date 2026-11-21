# @hoshinotatsuya/pg-saga

[![npm](https://img.shields.io/npm/v/@hoshinotatsuya/pg-saga.svg)](https://www.npmjs.com/package/@hoshinotatsuya/pg-saga)
[![CI](https://github.com/HoshinoTatsuya/pg-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/HoshinoTatsuya/pg-saga/actions/workflows/ci.yml)

[English](README.md) · [Изменения](CHANGELOG.md)

Оркестрация саг на чистом PostgreSQL поверх `@hoshinotatsuya/outbox` — без внешнего кластера. Шаги — команды через outbox (at least once), ответы — через inbox (идемпотентно), состояние экземпляра пишется в ТОЙ ЖЕ транзакции, что данные шага и следующая команда.

| Что | Как |
|---|---|
| определение | `defineSaga<Вход>(имя, { version }).step(…).remoteStep(…).build()` — выход шага вливается в контекст следующего, обращение к полю, которого шаги не дают, не компилируется |
| атомарность | одна транзакция на сообщение: отметка inbox → `SELECT … FOR UPDATE` экземпляра → шаг → состояние, журнал, следующая команда в outbox. Откатилась — не осталось ни эффекта шага, ни записи о нём, ни команды |
| локальный шаг | `execute`/`compensate` получают транзакцию шага (`tx`): их запись в базу атомарна с продвижением саги |
| удалённый шаг | команда участнику через outbox + таймер ответа (`deliverAt`); ответ — `handleReply` или событие `saga.reply` в том же outbox; команда, которую outbox не запишет никогда, — окончательный отказ шага |
| повтор | `retry` шага: попытки, пауза `min(крышка, база × 2^(n−1))` с половиной случайной; `SagaStepRejectedError` (или `retryable: false` в ответе) — без повторов |
| таймаут | локальный — `timeoutMs`, по умолчанию 12 с (вместе с ожиданием строки экземпляра и ROLLBACK — меньше потолка доставки релея): снимает и ожидание в процессе, и SQL шага — каждый запрос шага получает `statement_timeout`, равный ОСТАТКУ бюджета шага, — транзакция шага откатывается; удалённый — таймер-событие с `deliverAt`, исход по нему **неизвестен** |
| компенсации | завершённые шаги с компенсацией — строго в обратном порядке; после таймаута удалённого шага — и он сам |
| упала компенсация | повтор (`compensationRetry`), по исчерпании — `stuck`: экземпляр стоит и ждёт человека (`SagaAdmin`, CLI `saga-stuck`) |
| встал на DEAD | сторож `SagaStallWatch`: сообщение, которого экземпляр ждёт ИМЕННО по своему адресу, ушло в DEAD outbox — `blocked`, запись `delivery-dead`, хук `blocked`; ручной повтор — без порога простоя |
| конкурентность | один экземпляр двигает один исполнитель (блокировка строки); повторная доставка — `duplicate` (inbox), сообщение не той попытки/шага/поколения или прежнего воплощения того же id — `stale` |
| версии | экземпляр доезжает по версии, с которой начат; нет её в процессе или шаги разошлись — отказ доставки с причиной, экземпляр не трогается |
| наблюдаемость | хук `SagaMetrics` после фиксации перехода (и `blocked`/`stalled` сторожа); срез `SagaAdmin.stats` (активные, `stuck`, `blocked`, начато/завершено за окно, возраст старейшего, простой замёрзшего); отчёты о застрявших и о замёрзших |

## Установка

```sh
pnpm add @hoshinotatsuya/pg-saga @hoshinotatsuya/outbox @hoshinotatsuya/pg-runtime drizzle-orm pg
pnpm add -D @types/pg   # необязательно, для TypeScript
```

Peer-зависимости (точные диапазоны — `peerDependencies` в `package.json`): `@hoshinotatsuya/outbox` `^0.6.0 || ^0.7.0 || ^0.8.0`, `@hoshinotatsuya/pg-runtime` `^0.7.1 || ^0.8.0 || ^0.9.0`, `drizzle-orm` `>=0.45.2 <0.46`, `pg` `^8.18.0`, `@types/pg` `>=8.15.0` (необязательна). Node.js ≥ 24, PostgreSQL 15–18.

Все примеры кода ниже — файлы каталога [`examples/`](examples): `pnpm typecheck` проверяет их против исходников пакета и сверяет, что README цитирует их дословно.

## Подключение

Таблицы — миграцией потребителя: outbox и inbox из `@hoshinotatsuya/outbox`, свои — `sagaTablesSql` (или `defineSagaTables` в схеме drizzle-kit — те же таблицы, сверка по каталогу в спеке). Своя схема — `{ schema }` в обоих (и в `tables` оркестратора и админки). Имена индексов и ограничений по умолчанию — от имени таблицы (`<экземпляры>_status_updated_idx`, `<журнал>_saga_id_fkey` …); длиннее 63 байт — отказ (Postgres обрезал бы имя молча), и для длинных имён таблиц или своей конвенции их задаёт `indexNames` (`instancesPkey`, `instancesStatusIdx`, `instancesCreatedIdx`, `journalPkey`, `journalSagaFkey`, `journalSagaIdx`; в DDL и схеме drizzle одинаково). Неизвестный ключ, имя не по правилу `[a-z_][a-z0-9_]*` и совпадение имён — отказ до DDL.

<!-- example: examples/quick-start.ts -->
```ts
import {
  inboxTableSql,
  OUTBOX_TIMESTAMP,
  OutboxHandlers,
  OutboxInbox,
  OutboxRelay,
  OutboxStore,
  outboxTableSql,
} from '@hoshinotatsuya/outbox';
import { defineSaga, SagaOrchestrator, sagaTablesSql } from '@hoshinotatsuya/pg-saga';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { Pool } from 'pg';

// 1. Migration: the outbox and inbox tables come from the outbox package, the saga tables from this one.
export const migrationSql = [
  outboxTableSql({ name: 'app_outbox', timestamps: OUTBOX_TIMESTAMP.zoned }),
  inboxTableSql({ name: 'app_inbox', timestamps: OUTBOX_TIMESTAMP.zoned }),
  sagaTablesSql({ instances: 'saga_instances', journal: 'saga_journal' }),
  'CREATE TABLE IF NOT EXISTS reservations (id serial PRIMARY KEY, order_id text NOT NULL)',
].join(';\n');

// 2. Definition: a local step (runs in the saga's transaction) and a remote step (a command to a participant).
// `tx` is typed as an executor (`tx.execute(sql)`); with the default runner it is a drizzle transaction, so
// `(tx as unknown as NodePgDatabase).insert(...)` works as well.
export const orderSaga = defineSaga<{ orderId: string }>('order', { version: 1 })
  .step('reserve', {
    execute: async ({ tx, context }) => {
      const { rows } = await tx.execute(
        sql`INSERT INTO reservations (order_id) VALUES (${context.orderId}) RETURNING id`,
      );
      const [row] = rows as ReadonlyArray<{ id: number }>;
      if (row === undefined) {
        throw new Error('reservation was not inserted');
      }
      return { reservationId: row.id }; // merged into the context of the next steps
    },
    compensate: async ({ tx, context }) => {
      await tx.execute(sql`DELETE FROM reservations WHERE id = ${context.reservationId}`);
    },
    retry: { maxAttempts: 3, backoffMs: 1_000 },
    timeoutMs: 10_000,
  })
  .remoteStep<{ chargeId: string }>('charge', {
    command: ({ context, stepKey }) => ({
      eventType: 'billing.charge',
      payload: { orderId: context.orderId, idempotencyKey: stepKey },
    }),
    compensation: ({ executeStepKey }) => ({ eventType: 'billing.refund', payload: { executeStepKey } }),
    timeoutMs: 60_000,
  })
  .build();

// 3. Wiring: one outbox store, the orchestrator, the outbox handlers and a relay over the SAME store.
export const startSagaRuntime = (pool: Pool) => {
  const db = drizzle(pool);
  const outbox = new OutboxStore(db, { table: 'app_outbox' });
  const orchestrator = new SagaOrchestrator({
    db,
    outbox,
    inbox: new OutboxInbox(db, { table: 'app_inbox' }),
    tables: { instances: 'saga_instances', journal: 'saga_journal' },
  }).register(orderSaga);

  const handlers = new OutboxHandlers();
  orchestrator.registerHandlers(handlers); // saga.command, saga.timer, saga.reply
  // Remote participants are ordinary outbox handlers (see examples/participant.ts).
  const relay = new OutboxRelay({ store: outbox, deliver: handlers.deliver });
  relay.start(1_000);
  return { db, outbox, orchestrator, handlers, relay };
};

// 4. Start: inside the caller's transaction, so the instance and its first command appear with the order row.
export const placeOrder = async (
  { db, orchestrator }: ReturnType<typeof startSagaRuntime>,
  orderId: string,
): Promise<boolean> => {
  const result = await db.transaction((tx) =>
    orchestrator.start(tx, orderSaga, { id: `order:${orderId}`, input: { orderId } }),
  );
  return result.started; // false: an instance with this id already exists
};
```

`deps.outbox` — хранилище `OutboxStore` (нужен его `config`): `config.deliveryTimeoutMs` — потолок доставки релея, построенного над этим хранилищем, с ним `register` сверяет локальные шаги (см. «Определение»); типы событий саги (`eventTypes`) — непустые строки до 64 знаков (`event_type` outbox) и без правила слияния в `config.coalescing` (события саги идут со своим id) — иначе `SagaConfigError` в конструкторе.

Свои транзакции оркестратор открывает через `transactionKeepingFailure` из `@hoshinotatsuya/pg-runtime` (`sagaTransactionRunner`): соединение не утекает, обрыв после COMMIT — `CommitOutcomeUnknownError`, повтор на 40001/40P01, `lockTimeoutMs` (5 с) на ожидание любой блокировки в транзакции — и строки экземпляра, и блокировок в коде шага — и потолок ПРОСТОЯ транзакции (`idle_in_transaction_session_timeout`) = потолку доставки релея (`deliveryTimeoutMs` хранилища outbox). Без него реплика, у которой посреди шага замёрзла сеть, оставляла бы на сервере сессию «idle in transaction» с блокировкой строки экземпляра и ключом inbox до оттаивания сети или TCP keepalive сервера (часы): отмена и доставки экземпляра всё это время отказывают по ожиданию блокировки. Законный шаг этим потолком не обрывается: `register` держит локальный шаг короче потолка доставки, а у раннера `sagaTransactionRunner` с другим потолком простоя (`idleInTransactionTimeoutMs`, по умолчанию 20 с) — и короче него. Своя обёртка — `deps.transaction`: потолок простоя в ней — забота вызывающего (не меньше самого долгого локального шага).

Время перехода снимается ПОСЛЕ блокировки строки, в двух смыслах. Отметка (`updated_at`, журнал, `finished_at`, длительности) — не раньше последнего сдвига экземпляра: шаг, державший строку секундами, и реплика с отстающими часами не уводят её назад. СРОКИ (создание и доставка команды, пауза повтора, таймер ответа, `wake_at`) — от своих часов: зажим переносил бы в них часы реплики, ушедшей вперёд (или админки на ноутбуке), и команда ждала бы, пока реальное время их догонит.

`start` с тем же `id` второй раз ничего не делает (`{ started: false }`), пока экземпляр есть; после `prune` id можно занять снова. Воплощение — случайное 48-битное целое, выбранное при старте (`SagaInstance.incarnation`), — входит в id каждого его сообщения и в ключ шага, и сверка «ждёт ли экземпляр это сообщение» проверяет id, а не только адрес (поколение, фаза, шаг, попытка): таймер, команда и задержанный ответ ПРЕЖНЕГО экземпляра того же id новому не применяются — `stale`, без записи в его журнал. Воплощение не выводится из момента старта: при неподвижных часах (тесты) или `now` старта из времени события второй экземпляр того же id иначе получил бы те же id сообщений, outbox и inbox сочли бы его первую команду дублем, и сага не поехала бы никогда. Источник — `crypto.randomInt`, а не `deps.random` (его подменяют постоянным ради предсказуемой паузы повтора).

С outbox `ordering: OUTBOX_ORDERING.aggregate` сага работает так же. Каждый вид сообщения лежит в агрегате (`SAGA_AGGREGATE_TYPE`), выбранном по тому, кого он вправе держать:

| Сообщение | `aggregate_type` / `aggregate_id` | Кого может держать |
|---|---|---|
| немедленная команда исполнителю саги (шаг, компенсация, отмена и её уведомление, ручной повтор `SagaAdmin`) | `saga` / id экземпляра | только команды саги того же экземпляра: их порядок и так держат блокировка строки и сверка адреса, а устаревшая уходит `stale` |
| отложенная команда (пауза повтора) | `saga-deferred` / свой id | никого |
| таймер ответа удалённого шага | `saga-timer` / свой id | никого |
| ответ участника событием outbox (`sagaReplyMessage`) | `saga-reply` / свой id | никого |
| ответ участника на пакете 0.1.0 (смешанный флот) | `saga` / id экземпляра | немедленные команды саги этого экземпляра, пока ответ не применён (обновите участников до оркестратора — см. «Порядок выкатки») |
| команда участнику, по умолчанию | `saga-remote:<номер шага>` / id экземпляра | только команды ТОМУ ЖЕ шагу этого экземпляра: выполнение, его повторы и компенсацию одному участнику — возврат не обгоняет списание |
| команда участнику со своим `aggregateType` | свой тип / id экземпляра | все команды экземпляра с этим типом — одна очередь |

Команда лежащему участнику (повторы релея до DEAD — по умолчанию сутки) поэтому не держит ни команды самой саги (компенсации после таймера, отмену), ни команды ДРУГИМ участникам того же экземпляра: компенсация у живого участника идёт сразу после таймера лежащего. Её собственная компенсация (тот же шаг, тот же участник) ждёт её — так задуман порядок для участника; таймер компенсации отсчитывает это ожидание, и по исчерпании повторов экземпляр уходит в `stuck`, а не стоит молча. Команды разных шагов порядка через outbox не требуют: следующий шаг уходит только после ответа на предыдущий, компенсация нижнего — после компенсации верхнего.

FIFO outbox упорядочивает агрегат по `(created_at, id)`. Поэтому команда участнику записывается СТРОГО позже ждущего (недоставленного) события своего агрегата: иначе возврат, записанный репликой с отстающими часами (или в ту же миллисекунду при неподвижных часах), встал бы в очереди перед списанием. Цена — при расхождении часов такая команда ждёт, пока свои часы не дойдут до момента ждущего предшественника; без ждущего предшественника (и при `ordering` по умолчанию) — ожидания нет.

Свой `aggregateType` команды — участник, названный вами: разным участникам — разные типы (общий тип снова связал бы их очередью), выполнению и компенсации одного шага — один (иначе возврат может обогнать списание). `saga` — отказ шага без повторов (`SagaPayloadError`): команда лежащему участнику держала бы команды самой саги.

## Определение

Определение — `orderSaga` в примере выше: локальный шаг `reserve` с компенсацией, повтором и `timeoutMs`, удалённый шаг `charge` с командой компенсации. `tx` шага типизирован как исполнитель (`tx.execute(sql)`); с раннером по умолчанию это транзакция drizzle, поэтому работает и `(tx as unknown as NodePgDatabase).insert(...)`.

- Контекст — JSON-объект в `saga_instances.context`: выход шага проходит через JSON (Date в поле следующий шаг получит строкой; у экземпляра класса остаются его собственные поля). Что после JSON не объект — массив, примитив, объект с `toJSON()`, отдающим не объект (`Date` целиком, своя обёртка), — и BigInt и NUL — окончательный отказ шага. Так же — Map, Set, WeakMap, WeakSet, Promise и Error где угодно внутри: JSON молча превращает их в `{}` (у Error пропадают сообщение и стек), и следующий шаг получил бы пустой объект без отказа. Буквальный текст `\u0000` (обратная косая и пять знаков) — не NUL: jsonb его хранит, отказа нет. Та же проверка у входа `start`, данных ответа участника и `contextPatch` ручного повтора.
- Команда удалённого шага, которую outbox не запишет НИКОГДА, — окончательный отказ шага (одна попытка, компенсации, причина — в `failure`), а не повторы доставки до DEAD и вечный `running`. Это весь класс отказов записи: проверка события до записи (не JSON, BigInt, NUL, `payload` больше `maxPayloadBytes`, заголовки без опции `headers` у outbox или не той формы, id у типа со слиянием) и отказ данных на сервере (SQLSTATE класса 22: `eventType` или `aggregateType` длиннее `varchar(64)`; outbox 0.7 отказывает на них ещё до записи, с именем поля — исход тот же). Нет `eventType` — тоже. Отказ базы или связи при записи (обрыв, блокировка, сериализация) — не отказ шага: доставку повторит outbox.
- `stepKey` = `<sagaId>#<воплощение>:<фаза>:<номер шага>` — один во всех попытках и поколениях экземпляра и другой у нового экземпляра с тем же id (после `prune`, даже при тех же часах): ключ идемпотентности для внешней системы. Экземпляры, начатые версией 0.1.0, доживают с ключом `<sagaId>:<фаза>:<номер>` (и id сообщений её формата) — иначе повтор шага после выкатки пришёл бы к участнику под новым ключом и выполнился бы второй раз. В фазе компенсации шаг и построитель компенсации получают ещё `executeStepKey` — ключ выполнения: отмена находит операцию, даже если её исход неизвестен (таймаут ответа); участник видит его в `meta.executeStepKey`.
- Шаг получает ЗАМОРОЖЕННУЮ копию контекста: менять контекст можно только выходом шага (изменение вложенного поля на месте бросает `TypeError`).
- Тип контекста после шага — поля выхода шага ПЕРЕКРЫВАЮТ одноимённые прежние (`status: 'new'` → `'paid'`), как и в рантайме (`SagaContextMerge`). Вход и выход шага — тип-литерал, интерфейс, класс или объединение (размеченное объединение входа сохраняет поля вариантов на каждом шаге). Выход-словарь (`Record<string, X>`) прежних полей в типе не стирает: перекрываются только явные ключи выхода — что словарь в рантайме содержит тот же ключ, тип не видит. Данные ответа участника (`sagaReplyOf`) и `contextPatch` ручного повтора — тоже любой объектный тип (интерфейс-DTO). Компенсация локального шага получает контекст после шага; компенсация удалённого — `SagaUncertainMerge`: выход мог не прийти, поэтому новые поля необязательны, а перекрытые — прежнего ИЛИ нового типа. Компенсация получает ТЕКУЩИЙ контекст экземпляра: поле, которое позже перезаписал следующий шаг (уже компенсированный), придёт в его значении — на месте шага тип этого не видит.
- Шаг без компенсации при откате пропускается. Пустая сага, повтор имени шага, удалённый шаг без `timeoutMs` — отказ при определении.
- Локальный шаг без `timeoutMs` получает `SAGA_DEFAULTS.localStepTimeoutMs` (12 с). Бюджет доставки — `timeoutMs + lockTimeoutMs + rollbackTimeoutMs` (ожидание строки экземпляра и ROLLBACK после отказа; по умолчанию 12 + 5 + 2 = 19 с): он обязан быть меньше потолка доставки релея, иначе релей оборвёт доставку раньше записи отказа, и доставка будет повторяться до DEAD. Потолки берутся у того раннера, который их применяет: `sagaTransactionRunner` регистрирует свои `lockTimeoutMs`, `rollbackTimeoutMs` и потолок простоя; у своей обёртки (`deps.transaction`) — `deps.lockTimeoutMs` и `SAGA_DEFAULTS.rollbackTimeoutMs` (задавайте их равными потолкам обёртки). `lockTimeoutMs` и опции раннера — целые ≥ 1, иначе `SagaConfigError` в конструкторе. `register` сверяет это ВСЕГДА — с `deps.outbox.config.deliveryTimeoutMs` (у outbox по умолчанию 20 с), то есть с тем же числом, что применяет релей над этим хранилищем. Нарушение — `SagaConfigError` на старте. Время перехода (пауза повтора, `finished_at`, длительность) берётся ПОСЛЕ шага, а не до него.
- Определение, собранное мимо построителя, `register` проверяет так же: вид шага, функции `execute`/`command` (и компенсации), `timeoutMs` удалённого шага, политики повтора. Иначе переход споткнулся бы посреди доставки и повторял бы это до DEAD.

### Локальный шаг

Выполняется исполнителем саги внутри транзакции перехода. Его запись через `tx` фиксируется вместе с отметкой «шаг выполнен» и следующей командой — или не фиксируется вовсе: шаг ровно один раз **по эффекту в базе**. Побочное действие вне базы (HTTP, письмо) — at least once: процесс может упасть после него и до COMMIT. Отказ шага откатывает транзакцию (и его эффекты), отказ записывается второй транзакцией: повтор с паузой или компенсации.

Не всякий отказ — отказ шага. Обрыв соединения, неизвестный исход COMMIT, таймаут ожидания строки ЭКЗЕМПЛЯРА и потолок доставки релея уходят наружу без записи: outbox повторит доставку, inbox (если транзакция всё-таки зафиксировалась) скажет `duplicate`, иначе шаг выполнится заново. Попытка шага при этом не тратится.

А вот отказ ВНУТРИ кода шага — отказ шага, с повтором: в том числе таймаут блокировки, которую ждал сам шаг (`55P03`), отмена SQL по потолку шага (`57014` — записывается как таймаут) и ошибка SQL, которую шаг поймал и проглотил: транзакция после неё прервана (`25P02`), запись перехода падала бы так на каждой доставке — это тоже отказ шага, а не инфраструктуры.

Шаг, брошенный по таймауту или отменой доставки, продолжает исполняться (JS не прерывает начатый вызов; шаг получает `signal` — слушайте его). Его транзакцию оркестратор откатил, а `tx` шага закрыт: любое обращение к нему после конца шага — отказ ДО обращения к соединению. Без этого свой `deps.transaction` без защиты соединения (`db.transaction` drizzle) отдал бы запрос брошенного шага соединению, уже вернувшемуся в пул, — мимо транзакции или в чужую, и эффект попытки, записанной отказом, лёг бы в базу рядом с эффектом повтора. Ограда стоит на объекте `tx`: объекты, полученные из него до конца шага (вложенная транзакция, построитель, подготовленный запрос), закрывает только защита соединения раннера — у `sagaTransactionRunner` (`transactionKeepingFailure`) она есть.

Потолок запроса шага — остаток бюджета шага: перед каждым запросом транзакции шага `statement_timeout` ставится в то, что осталось до `timeoutMs` (свой меньший потолок, поставленный шагом, сохраняется). Иначе запрос, начатый за миг до потолка, шёл бы на сервере весь потолок: ROLLBACK встал бы за ним, запись отказа не дождалась бы строки экземпляра, и шаг повторялся бы на каждой доставке до DEAD. Отмену с другого соединения (`pg_cancel_backend`) пакет не использует: под пулером в transaction mode она может попасть в чужой запрос и требует свободного соединения. У транзакции drizzle потолок ставится на соединении её сессии — `execute`, построители (`tx.insert(...)`), вложенная транзакция; у своей транзакции не drizzle — перед каждым `execute`. Реляционные запросы `tx.query.*` держат сессию со своего построения: им остаётся потолок шага целиком. Цена — один короткий запрос `set_config` перед каждым запросом шага.

Текст отказа в журнале и `last_error` — код и сообщение сервера по цепочке `cause`; текст обёртки драйвера (`Failed query: … params: …`) с параметрами запроса туда не пишется. Текст строится из любого брошенного значения (объект без прототипа, сломанный `toString`, Proxy, ошибка из другого контекста — по форме, с именем); NUL удаляется, непарная половина суррогатной пары заменяется на U+FFFD — так же у текста отказа в ответе участника (`sagaReplyOf`).

### Удалённый шаг и участник

Команда уходит событием outbox с `payload = { sagaCommand: meta, data }` и таймером ответа. Участник:

<!-- example: examples/participant.ts -->
```ts
import type { OutboxDeliveryPort, OutboxHandlers, OutboxInbox, OutboxStore } from '@hoshinotatsuya/outbox';
import { parseSagaCommand, sagaReplyMessage, sagaReplyOf } from '@hoshinotatsuya/pg-saga';
import { eq } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { pgTable, text } from 'drizzle-orm/pg-core';

const charges = pgTable('charges', { stepKey: text('step_key').primaryKey(), chargeId: text('charge_id').notNull() });

/**
 * A participant of the remote step `charge` living in the same database: its effect is applied once per
 * `stepKey` (inbox), and the reply is an outbox event written in the same transaction as the effect.
 */
export const billingParticipant = (db: NodePgDatabase, outbox: OutboxStore, inbox: OutboxInbox) => {
  const charge: OutboxDeliveryPort = async (delivery) => {
    const { meta } = parseSagaCommand(delivery.payload);
    await db.transaction(async (tx) => {
      await inbox.once(tx, 'billing', meta.stepKey, async () => {
        await tx.insert(charges).values({ stepKey: meta.stepKey, chargeId: `ch_${meta.sagaId}` });
      });
      // Reply to a duplicate as well (same outcome again): duplicates arrive after a reply timeout and
      // after a manual retry, and silence would leave the saga waiting for a reply that never comes.
      const [row] = await tx.select().from(charges).where(eq(charges.stepKey, meta.stepKey));
      const reply =
        row === undefined
          ? sagaReplyOf(meta, { ok: false, error: 'charge not recorded', retryable: true })
          : sagaReplyOf(meta, { ok: true, data: { chargeId: row.chargeId } });
      await outbox.enqueue(tx, [sagaReplyMessage(reply)]);
    });
  };
  const refund: OutboxDeliveryPort = async (delivery) => {
    const { meta } = parseSagaCommand(delivery.payload);
    await db.transaction(async (tx) => {
      // The compensation must tolerate "nothing to undo": after a reply timeout the charge may not exist.
      await tx.delete(charges).where(eq(charges.stepKey, meta.executeStepKey ?? meta.stepKey));
      await outbox.enqueue(tx, [sagaReplyMessage(sagaReplyOf(meta, { ok: true }))]);
    });
  };
  return { charge, refund };
};

export const registerBilling = (
  handlers: OutboxHandlers,
  ports: ReturnType<typeof billingParticipant>,
): OutboxHandlers =>
  handlers
    .register('billing.charge', (_payload, delivery, context) => ports.charge(delivery, context))
    .register('billing.refund', (_payload, delivery, context) => ports.refund(delivery, context));
```

Команда компенсации уходит своим типом события (`compensation` шага): участник, принимающий оба типа одним портом, обязан различать их по типу или `meta.phase`.

Участник в другой системе отвечает своим транспортом, а приёмник вызывает `orchestrator.handleReply(reply)`. Правила ответа:

- ответ применяется не больше раза на команду (`commandId` — ключ inbox): дубль — `duplicate`;
- `commandId` ответа обязан быть id команды ЭТОГО экземпляра (его воплощения) для адреса ответа: ответ прежнего экземпляра того же id (до `prune`) или с выдуманным id — `stale` без записи в журнал (и для локального шага — не «вид шага сменили», а `stale`);
- отказ (`ok: false`) прежней попытки, когда уже ждут следующую, — `stale`; **успех** прежней попытки того же шага принимается (шаг выполнен, повтор у участника отсечёт его идемпотентность по `stepKey`);
- данные успешного ответа, которые нельзя сохранить в контекст (BigInt, NUL, не объект), — отказ шага БЕЗ повторов и с неизвестным исходом: компенсируется и сам шаг;
- ответ или таймер экземпляру, которого уже нет (удалён `prune`), — `stale` с предупреждением в лог, а не повторы доставки до DEAD;
- `commandId` принимается в любом регистре (участник на платформе, форматирующей GUID заглавными) и приводится к нижнему;
- окончательный отказ узнаётся и обёрнутым: `SagaStepRejectedError` где угодно в цепочке `cause` (и из другой копии пакета);
- таймер отсчитывается от записи команды, а не от доставки участнику: отставание релея съедает `timeoutMs`. Берите `timeoutMs` больше худшего отставания релея плюс времени участника;
- исход удалённого шага неизвестен, а компенсации у шага нет — компенсации проходят мимо него, а в журнале запись `uncompensated-uncertain`: эффект у участника мог остаться;
- таймер сработал — исход НЕИЗВЕСТЕН: участник мог выполнить шаг, а ответ потеряться. По исчерпании повторов компенсируется и этот шаг: компенсация участника обязана переносить «отменять нечего».
- успех, которого экземпляр уже не ждёт, — `stale`; запись `late-reply` в журнале — только когда сага этот эффект НЕ учла: команда ровно этой попытки и поколения уходила (`step-dispatched` в журнале), а шаг в этой фазе не выполнен НИ ОДНОЙ попыткой (нет `step-completed` для выполнения, `step-compensated` для компенсации). Это успех шага после начала отката, успех компенсации у застрявшего (ручной возврат был бы вторым), успех поколения до ручного повтора. Дубль шага или компенсации, уже выполненных другой попыткой или поколением, — у идущей, откатывающейся, откатанной или завершённой саги — не событие и не пишется; ответ на команду, которой не было (компенсация у идущей саги), — тоже. Если компенсация дошла до участника РАНЬШЕ запоздавшего выполнения, развести их может только сам участник (например, запись-надгробие при компенсации, которую выполнение проверяет). Библиотека это видит, но решить за участника не может.

## Версии

Экземпляр хранит версию и имена шагов, с которыми начат. Оркестратор держит несколько версий одной саги (`register(v1).register(v2)`); новые экземпляры начинаются тем определением, которое передали в `start`.

- Версии экземпляра нет в процессе (выкатка: экземпляр v2 пришёл на старую реплику) — `SagaDefinitionUnavailableError`, хук `definitionUnavailable`, экземпляр не тронут; outbox повторит доставку, и её примет реплика с этой версией. `deadAfterMs` outbox держите больше длительности выкатки.
- Шаги переименованы, переставлены или их стало меньше без смены версии — тот же отказ с причиной «разошлись». Экземпляр не поедет по чужим шагам молча.
- Удалённый шаг стал локальным без смены версии — ответ участника даёт тот же отказ и хук с причиной «вид шага сменили».
- Не видно без смены версии: появление или исчезновение компенсации, смена политики повтора, смена локального шага на удалённый (экземпляр хранит только имена шагов). Любая такая правка — новая версия.
- Конечному экземпляру определение не нужно: старую версию можно удалить из кода, когда активных экземпляров с ней нет (`SagaAdmin.list({ status })`).

## Застрявшие и ручной разбор

Разбор, отмена и сторож простоя:

<!-- example: examples/operations.ts -->
```ts
import type { OutboxStore } from '@hoshinotatsuya/outbox';
import { SagaAdmin, type SagaOrchestrator, SagaStallWatch, sagaTransactionRunner } from '@hoshinotatsuya/pg-saga';
import { sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

const tables = { instances: 'saga_instances', journal: 'saga_journal' };

// Cancel a running saga from application code, in the caller's transaction (like `start`). Set your own
// lock_timeout: the instance row may be held by a running step.
export const cancelOrder = (db: NodePgDatabase, orchestrator: SagaOrchestrator, orderId: string) =>
  db.transaction(async (tx) => {
    await tx.execute(sql`SET LOCAL lock_timeout = '5s'`);
    return orchestrator.cancel(tx, `order:${orderId}`, 'customer cancelled the order');
    // { cancelled: true, status: 'compensating' } or { cancelled: false, status } when not running
  });

// Manual repair of stuck/blocked/stalled instances (the `saga-stuck` CLI does the same from a shell).
export const repair = async (
  db: NodePgDatabase,
  outbox: OutboxStore,
  orchestrator: SagaOrchestrator,
  id: string,
) => {
  const admin = new SagaAdmin({
    db,
    transaction: sagaTransactionRunner(db),
    outbox,
    tables,
    commandEventType: orchestrator.eventTypes.command, // required by retry
  });
  const stuck = await admin.stuckReport(); // stuck and blocked: what failed, on which step, for how long
  const stalled = await admin.stalledReport(30 * 60_000); // active, not moved for >= 30 min
  const shown = await admin.show(id); // instance + journal in order
  await admin.retry(id, 'warehouse is back'); // stuck/blocked: retry the step in its phase, new generation
  await admin.retry(id, 'amount fixed', { contextPatch: { amount: 100 } }); // patch the context first
  await admin.retry(id, 'dead letter handled', { stalledForMs: 30 * 60_000 }); // a stalled active instance
  await admin.resolve(id, 'refund issued by hand'); // terminal `resolved`, the note goes to the journal
  const removed = await admin.prune(new Date(Date.now() - 30 * 86_400_000)); // finished ones, journal cascades
  return { stuck, stalled, shown, removed, stats: await admin.stats(new Date(Date.now() - 86_400_000)) };
};

// The stall watch moves an instance whose expected message went DEAD in the outbox to `blocked`.
export const runStallWatch = (db: NodePgDatabase, outbox: OutboxStore): NodeJS.Timeout => {
  const watch = new SagaStallWatch({
    db,
    transaction: sagaTransactionRunner(db),
    outbox,
    tables,
    stalledAfterMs: 30 * 60_000,
    metrics: {
      blocked: (event) => console.warn('saga blocked', event.sagaId, event.error),
      stalled: (event) => console.warn('saga stalled', event.sagaId, event.stalledForMs),
    },
  });
  return setInterval(() => {
    watch.runOnce().catch((error: unknown) => console.error('stall watch failed', error));
  }, 60_000);
};
```

**Предел `prune`.** Срок `olderThan` берите больше худшего срока доставки outbox (повторы до DEAD — по умолчанию сутки). Отмена (`cancel`), завершившая экземпляр сразу, хук `metrics.compensated` вызывает уведомлением через outbox. Если `prune` удалил экземпляр раньше, чем релей доставил это уведомление, хук не вызовется, а в лог уйдёт предупреждение «сообщение экземпляру, которого нет (удалён prune?)». Само состояние при этом верное: экземпляр уже завершён.

Отмена идущей саги — из кода приложения, в его транзакции (как `start`; `cancelOrder` выше): `{ cancelled: true, status: 'compensating' }` — новое поколение, компенсации выполненных шагов (и текущего удалённого: команда могла уйти); не running — `{ cancelled: false, status }`.

`cancel` (и `start` с занятым id) ждёт строку экземпляра в ТРАНЗАКЦИИ ВЫЗЫВАЮЩЕГО, а её потолки пакет не ставит: без `lock_timeout` отмена повиснет, пока строку держит шаг (до его потолка) или — при замёрзшей сети реплики — сессия «idle in transaction» (до потолка простоя оркестратора, `deliveryTimeoutMs`). Ставьте потолки своей транзакции сами, например `transactionKeepingFailure(db, work, { lockTimeoutMs: 5_000, idleInTransactionTimeoutMs: 20_000 })` из `@hoshinotatsuya/pg-runtime` или `SET LOCAL lock_timeout = '5s'` первой инструкцией; отказ по нему (`55P03`) — повторите отмену позже.

Текущий удалённый шаг без компенсации при отмене — запись `uncompensated-uncertain` (как при таймауте): команда могла уйти, а отменить нечем. Компенсировать нечего — `{ cancelled: true, status: 'compensated' }` сразу; хук `compensated` всё равно вызывается — оркестратором, после фиксации его транзакции (отмена пишет ему уведомление): транзакция вызывающего может откатиться, и хука в ней нет. Новое поколение сверх int4 — `SagaConfigError` до записи.

`start` с занятым id отвечает `{ started: false, existing: { name, version, status } }` — видно, повтор ли это старта той же саги или id занят другой.

Вставший экземпляр (`blocked`) — тот, чьё ОЖИДАЕМОЕ сообщение ушло в DEAD outbox на отказе инфраструктуры. Сам он об этом не узнает — его находит сторож:

Сторож — `runStallWatch` в примере выше (или по cron на одной-двух репликах).

- Проход берёт до `batchSize` (100) неподвижных активных (после `wake_at`) по кругу — курсор по моменту последнего движения и id в памяти процесса — и ищет в DEAD outbox (`deadLetters({ ids })`, первичный ключ) ровно те сообщения, которых экземпляр ждёт по своему адресу: команду шага его поколения, фазы, шага и попытки или таймер ответа удалённого шага. id выведен из адреса и воплощения, поэтому DEAD прошлой попытки или поколения живой экземпляр не останавливает. Колонки момента DEAD в outbox нет, поэтому курсор идёт по стороне саги.
- Нашлось — своей транзакцией, под блокировкой строки и только если экземпляр всё ещё ждёт тот же адрес: `blocked`, `last_error` — причина DEAD, запись `delivery-dead` (тип и id сообщения, причина), после фиксации хук `blocked`. Второй сторож увидит уже `blocked` и ничего не сделает.
- Неподвижный дольше `stalledAfterMs` без DEAD (бэклог релея, долгий шаг) — хук `stalled`, один раз на эпизод в процессе (новый процесс скажет ещё раз). Повтора сторож НЕ делает.
- `SagaAdmin.retry` и `resolve` над `blocked` — без порога простоя: шаг повторяется в той фазе, где экземпляр встал (из `delivery-dead` его поколения), новым поколением. DEAD-сообщения саги не возвращайте `requeueDead`: экземпляр в `blocked` применит его как устаревшее; повторяйте экземпляр. `cancel` вставшего не отменяет (`{ cancelled: false, status: 'blocked' }`) — сначала `retry` или `resolve`.

Замёрзший экземпляр без сторожа — активный (`running`/`compensating`), чья доставка ушла в DEAD на отказе инфраструктуры: в `stuckReport` он не попадёт. Его показывают `stalledReport` (с `status`; старейшие по последнему движению первыми — позже из `updated_at` и `wake_at`) и `stats().longestIdleActiveMs`. Экземпляр, штатно ждущий паузы повтора или таймера ответа (колонка `wake_at` — срок, до которого он ждёт), замёрзшим не считается, пока срок не прошёл. Завершённые и застрявшие в отчёт о замёрзших не попадают. Повтор и разбор замёрзшего — только явно, с порогом простоя (`stalledForMs`): иначе живой долгий шаг запустился бы второй раз параллельно. Пороги простоя (`stalledReport`, `stalledForMs`) и сроки CLI — целые мс от 1 до `SAGA_LIMIT.durationMaxMs` (десять лет), общий потолок у API и CLI. Бэклог релея (команда ещё не доставлена, но не в DEAD) от замёрзшего отчёт не отличает: порог берите больше худшего отставания релея.

CLI: `saga-stuck <list|show|retry|resolve|stats|prune> --instances <т> --journal <т> [--outbox <т>] [--command-type <тип>] [--id] [--note] [--context-patch <json> | --context-patch-file <путь>] [--limit] [--stalled-ms <мс>] [--since-ms <мс>] [--older-than-ms <мс>] [--yes]`, `--help` — справка; адрес — `SAGA_DATABASE_URL` или `DATABASE_URL`. `retry` требует `--outbox` и `--command-type` (тип команды оркестратора: угаданный тип при своих `eventTypes` ушёл бы в DEAD). `list --stalled-ms N` — замёрзшие; `retry`/`resolve --stalled-ms N` — и над замёрзшим. `stats [--since-ms N]` — срез JSON (по умолчанию за сутки); `prune --older-than-ms N` — удаление завершённых (для планировщика). `retry`/`resolve`/`prune` без `--yes` ничего не меняют. Транзакция CLI — с потолками (`lock_timeout` 5 с, простой 20 с): строку экземпляра держит идущий шаг — отказ с кодом 1, а не ожидание без конца. Числа — только цифрами (`1e3` — отказ), `--limit` не больше 10 000, сроки — не больше `SAGA_LIMIT.durationMaxMs`. Пустая или пробельная `--note` у `retry` — причины нет (в журнал не пишется). `retry --context-patch '{"amount":100}'` (или `--context-patch-file` — данные не попадают в список процессов) — поправка контекста перед повтором, как `contextPatch` у `SagaAdmin.retry`: поля поверх прежних, ключи — в журнал; без `--yes` печатаются только ключи поправки, не значения. Поправка — JSON-объект хотя бы с одним полем, с той же проверкой, что выход шага; иначе, и у других команд, — код 2 до подключения. `--schema` один на таблицы саги и outbox.

`stuck` не теряется молча: экземпляр стоит, хук `stuck` вызван, `stats` считает его, `stuckReport` показывает. Замёрзший тоже виден — `stalledReport` и `longestIdleActiveMs`. Ручной повтор поднимает поколение: команды и таймеры прошлого поколения больше не применяются.

## Наблюдаемость

- `SagaMetrics` (после фиксации перехода; отказ хука проглатывается с предупреждением): `completed` (с длительностью), `compensated` (один раз на экземпляр — и после отмены, даже если компенсировать было нечего), `stuck`, `stepFailed` (`willRetry`), `blocked` и `stalled` (их зовёт `SagaStallWatch`), `stepCompleted` (`sinceLastMoveMs` — от предыдущего сдвига экземпляра), `definitionUnavailable` (счётчик ОТКАЗАННЫХ ДОСТАВОК: вызывается на каждый повтор релея). Хуков `started` и `cancelled` нет сознательно: старт и отмена идут в транзакции вызывающего, и хук засчитал бы откатившееся. `SagaAdmin` — свои хуки `manualRetry`/`manualResolve` (`deps.metrics`), тоже после фиксации.
- `SagaAdmin.stats(since)` — на каждую сагу: `running`, `compensating`, `stuck`, `blocked`, `started`/`completed`/`compensated`/`resolved` с момента `since`, `oldestActiveAgeMs` (по созданию; не меньше 0 — экземпляр могла начать реплика с часами впереди, как и `stuckForMs` отчётов), `longestIdleActiveMs` (по последнему сдвигу running/compensating — алерт «сага замёрзла»; долгую сагу от замёрзшей отличает он, а не возраст). Это источник гаугов: «запущено» считается по базе, потому что `start` идёт в транзакции вызывающего и хук из неё засчитал бы откатившийся старт.
- Журнал `saga_journal` — каждое событие экземпляра: старт, отправка команды, успех/отказ/таймаут шага, компенсации, `stuck`, ручные действия, поздние ответы.

## Граница с durable execution

Это не Temporal и не его замена. Чего здесь нет — и что такие системы дают сверх:

- **Детерминированный replay кода.** Там процесс саги — обычная функция, состояние восстанавливается переигрыванием истории событий; ветвления, циклы и локальные переменные сохраняются сами. Здесь — фиксированный список шагов и JSON-контекст; ветвление делается внутри шага, а не между ними.
- **Таймеры произвольного кода.** Там `sleep(30 дней)` в середине функции; здесь таймер — только «ответа на шаг нет за `timeoutMs`» и пауза повтора.
- **Видимость и история.** Там — UI с историей каждого исполнения, поиск по атрибутам, переигрывание. Здесь — две таблицы, `SagaAdmin` и CLI.
- **Версионирование кода внутри исполнения** (`patched`/`getVersion`) — здесь версия целиком на экземпляр.
- **Сигналы и запросы к работающему исполнению, дочерние исполнения, масштаб на тысячи исполнений в секунду с шардированием истории.**

Что даёт этот пакет: атомарность шага с его записью и командой в той же базе, где живут данные; ни одного нового сервиса; всё видно SQL-запросом. Если шагов десятки, ветвлений много или нужны месячные таймеры — это задача для durable-execution движка.

## Чего библиотека не делает

- exactly-once вне базы: внешний вызов внутри шага — at least once, ключ — `stepKey`;
- порядок между разными экземплярами (только внутри экземпляра);
- изоляцию компенсаций от параллельных саг над теми же данными (семантические блокировки — забота шагов);
- чистку inbox: `inbox.prune` с запасом больше худшего срока повторной доставки outbox;
- выделенную схему для outbox в CLI: `--schema` один на таблицы саги и outbox;
- чтение времени при `DateStyle`, отличном от ISO (разбор — текста Postgres в формате ISO: так его отдаёт сервер по умолчанию);
- сверку `deps.outbox` с базой саги: outbox обязан быть той же базы (иначе команда ручного повтора уйдёт мимо).

## Проверка

`pnpm test` поднимает postgres:17 в docker (`PG_TEST_IMAGE` — другой образ); база каждого набора — под ролью БЕЗ суперправ (проверяется фикстурой), релей — настоящий `OutboxRelay` с управляемыми часами. Набор проходит на Postgres 15, 16, 17, 18 и через PgBouncer в transaction mode (`PG_TEST_URL` — адрес пулера). Поддерживаемые версии — Postgres 15+. `pnpm test:consumers` — собранный пакет в проектах commonjs, node16, nodenext, bundler+ESM, nodenext+ESM (tsc, ts-jest, загрузка входа).

```sh
pnpm install
pnpm typecheck        # исходники, примеры и дословность примеров в README
pnpm lint
pnpm test             # нужен Docker: поднимает postgres:17-alpine на свободном порту
pnpm test:consumers   # собранный пакет в одноразовых проектах commonjs/node16/nodenext/ESM
pnpm build
```

`pnpm test` гоняет Jest через `jest-groups`: группы не больше 300 тестов, строго одна за другой, не больше 2 воркеров в группе. `PG_TEST_URL` — готовый сервер (так в CI).

| Свойство | Спека | Как проверено |
|---|---|---|
| счастливый путь, контекст копится | `saga-orchestrator.db` | контекст после всех шагов, журнал по порядку, хук `completed` один раз |
| отказ на каждом шаге | `saga-orchestrator.db` | `it.each` по 4 шагам: множество компенсаций И их порядок; эффекты отказавшего шага откатились |
| повтор с паузой | `saga-orchestrator.db` | паузы 750 и 1500 мс в `next_attempt_at` команд, эффект шага один |
| таймаут локального шага | `saga-orchestrator.db` | `step-timed-out` × 3, затем компенсации |
| упала компенсация → повтор → stuck | `saga-orchestrator.db` | 3 × `compensation-failed`, `saga-stuck`, хук один раз, отчёт; ручной повтор → `compensated` |
| удалённый шаг | `saga-remote.db` | ответ вливает данные, таймер после ответа — без эффекта; таймаут → повтор; неизвестный исход → компенсируется и сам шаг; окончательный отказ — нет |
| дубль ответа | `saga-remote.db` | два одновременных применения: `{applied, duplicate}`, сдвиг один |
| ответ и таймер прежней попытки | `saga-remote.db` | успех прежней — принят, отказ и таймер прежней — `stale`; поздний успех — `late-reply` |
| две реплики | `saga-concurrency.db` | 16 саг двумя релеями на своих пулах: множество эффектов = ожидаемому, каждый ровно раз; работали обе |
| одна команда двум репликам | `saga-concurrency.db` | гонка выстроена по `pg_stat_activity`: `{applied, duplicate}`, эффект один |
| блокировка экземпляра | `saga-concurrency.db` | успех и таймер одной попытки на двух репликах: таймер ждёт строку и видит сдвиг — `stale`, повтора нет |
| обрыв соединения посреди шага | `saga-failures.db` | `pg_terminate_backend` из-под суперпользователя: эффект откатился, попытка шага не потрачена, шаг один |
| смерть процесса посреди шага | `saga-failures.db` | код встал, соединение оборвано, захват брошен: новый процесс после окна перехвата доводит сагу, проснувшийся старый ничего не пишет |
| `CommitOutcomeUnknownError` | `saga-failures.db` | обе реальности — COMMIT прошёл / не прошёл, а вызывающему «исход неизвестен» (обёртка над настоящей транзакцией): шаг ровно один, сага завершена |
| атомарность шага | `saga-failures.db` | откат транзакции шага после записи перехода: ни эффекта, ни журнала, ни сдвига, ни команды |
| версии | `saga-versioning.db` | без v1 — отказ с причиной и хук, экземпляр не тронут; с v1 — доезжает по v1; шаги разошлись — отказ; конечному определение не нужно |
| схема | `saga-tables.db` | каталог = drizzle по колонкам с ТИПАМИ, NOT NULL, DEFAULT, identity, PK, индексам, внешнему ключу |
| разбор и CLI | `saga-admin.db` | `stats` (с `longestIdleActiveMs`), `prune` с каскадом журнала и потолком пачки сквозь статусы, `stalledReport` и повтор замёрзшего только явно, `resolve` живого running без порога — отказ, порог простоя 0 и меньше — отказ API, `retry` без типа команды — отказ до записи; CLI: неверный вызов — 2 без подключения, `--yes`, retry/resolve, `--stalled-ms` |
| тот же id после prune | `saga-orchestrator.db` | новый экземпляр доезжает: id сообщений с моментом создания не совпадают с прежними |
| потолок SQL шага | `saga-orchestrator.db` | `statement_timeout` = `timeoutMs` внутри шага (и по умолчанию 12 с); `pg_sleep(30)` при 200 мс — `step-timed-out`, а не 30 с ожидания |
| проглоченная ошибка SQL | `saga-orchestrator.db` | `25P02` после шага — отказ шага с повтором, сага доезжает |
| FIFO outbox по агрегату | `saga-remote.db`, `saga-fifo.db` | `ordering: aggregate`: удалённый шаг без ложного таймаута, одна попытка; лежащий участник не держит компенсаций после таймера и отмены; списание с реплики, чьи часы впереди на 5 мин, не обгоняется возвратом с верной; списание и возврат в одну миллисекунду — возврат на 1 мс позже; ДВА участника — лежащий B (шаг без компенсации) не держит компенсацию у живого A: `compensated` в ту же секунду, что таймер B; возврат не обгоняет списание; агрегат каждого вида сообщения; свой `aggregateType` — принят, `saga` — отказ шага |
| prune до срока таймера | `saga-remote.db` | таймер удалённого экземпляра — `published`, а не повторы и DEAD |
| команда/ответ, которых не записать | `saga-unwritable.db`, `saga-remote.db`, `saga-enqueue-rejected` | payload больше `maxPayloadBytes`, заголовки без опции, `eventType`/`aggregateType` длиннее 64, BigInt, NUL, нет `eventType` — одна попытка, компенсации; буквальный `\u0000` — уходит; отказ базы или связи — не отказ шага (таблица SQLSTATE и проводка в оркестраторе: обрыв 08006, обрыв без SQLSTATE, голый `TypeError` своей обёртки — попытка не потрачена, повтор доводит); NUL в данных ответа — отказ с неизвестным исходом; Map, Set, Error в выходе — отказ, класс с полями и `Date` — нет |
| прежнее воплощение того же id | `saga-incarnation.db`, `saga-message-belongs` | таймер и задержанный ответ прежнего экземпляра — `stale`, данные не вливаются; тот же id после `prune` при НЕПОДВИЖНЫХ часах и явном `now` старта — едет, ключи шагов разные; летящие сообщения 0.1.0 (id без воплощения, сверены с литералами её формулы) у её экземпляров — применяются, у новых — нет; произвольный id у экземпляра 0.1.0 — нет |
| время после блокировки строки | `saga-time.db` | cancel, отправка команды, ответ, таймер, ручной повтор и разбор, пока строку держат 15 с по часам: `updated_at` и журнал — после ожидания; отстающие часы реплики не уводят время назад (cancel, retry, resolve); часы ДРУГОЙ реплики впереди на 5 мин — сроки команд, таймера и паузы по своим часам (cancel, отправка, ответ, таймер, ручной повтор), отметка не назад; сага доезжает за секунды |
| поздний ответ: событие и двойники | `saga-late-reply`, `saga-late-reply.db` | таблица 7 статусов × 2 поколения × 2 фазы × 2 шага × 4 состояния журнала; на настоящих историях дубль пройденного шага, дубль выполненной компенсации, успех прежнего поколения у завершённой — без записи; неучтённый успех — с записью; поздний успех компенсации ВЫПОЛНЕННОГО шага у stuck — с записью; текст у разобранного вручную — без «после отката» |
| отмена | `saga-cancel.db`, `saga-transitions` | компенсируются только выполненные (не текущий локальный); удалённый без компенсации — `uncompensated-uncertain`; хук `compensated` один раз и когда компенсировать нечего; уведомление прежнего поколения — `stale` без хука; не running (5 статусов) и нет экземпляра — ничего не меняет; отказ шага во время отмены не записывается; поколение сверх int4 — отказ |
| типы контекста у потребителя | `saga-context-types` | `tsc --strict` реалистичного использования: вход и выход — интерфейсы и классы, компенсации шага, меняющего тип поля, обобщённая фабрика, вход-объединение (поля вариантов на каждом шаге), ответ и `contextPatch` — интерфейсы-DTO, выход-словарь не стирает прежних полей, `@ts-expect-error` на недоступном |
| летящие данные 0.1.0 | `saga-upgrade-sql.db` | до доводки ждущие сроков выглядят замёрзшими; `sagaUpgradeSql` заполняет `wake_at` и переносит агрегаты, повторно ничего не меняет; после — FIFO не держит, обе саги доезжают; устаревший `wake_at` (ответ применила 0.1.0, доставка следующего шага в DEAD) — сброшен, экземпляр виден замёрзшим, честно ждущий таймера — нет; outbox `naiveLocal` с зоной писателя — срок верен при другой зоне сессии; немедленная команда — `wake_at` NULL, сообщение на повторах доставки срока не даёт (команда на повторах видна замёрзшей, срок таймера, записанный 0.2.0, сохранён), отложенная — свой срок |
| FIFO по агрегату со справедливым захватом | `saga-fifo.db` | весь набор FIFO — и без `fairness`, и с `fairness: aggregate-type` (режим захвата проверяется отдельной проверкой) |
| порядок команд участнику на outbox без зоны | `saga-naive-outbox.db` | обе формы outbox (`zoned`, `naiveLocal`) при зоне сессии, отличной от зоны процесса: опережение агрегата — ровно 300 000 мс, возврат с отстающих часов записан на 1 мс позже списания и доставлен после него |
| брошенный шаг пишет через `tx` | `saga-guards.db` | шаг, глухой к сигналу, после таймаута пишет через `tx`: отказ, эффекта попытки нет, повтор доезжает — с раннером саги и со своим раннером `db.transaction` drizzle |
| часы разбора позади записи | `saga-admin.db` | `stuckForMs` и `oldestActiveAgeMs` — 0, а не минус; двойник при верных часах — точная разность |
| поправка контекста из CLI | `saga-admin.db` | `retry --context-patch` без `--yes` печатает только ключи; с `--yes` — поля поверх прежних, ключи в журнале; `--context-patch-file`; неверная поправка и чужая команда — код 2 без подключения |
| свои имена индексов | `saga-tables.db` | длинные имена таблиц: каталог = схема drizzle, имена — заданные; неизвестный ключ, негодное имя, совпадение с таблицей или индексом, незаданное длинное — отказ с подсказкой |
| порядок выкатки | `saga-rollout.db` | участник на разборщике 0.1.0: экземпляру 0.1.0 выполнение и компенсация уходят ключом её формата — разбираются, сага доезжает; экземпляру 0.2.0 — отвергается каждая команда; команда админки 0.1.0 экземпляру 0.2.0 — `stale` с предупреждением, чужая попытка — без |
| замёрзшая сеть реплики | `saga-frozen-network.db` | FaultProxy замораживает сеть посреди шага: сессия «idle in transaction» снята потолком простоя, отмена проходит, пока сеть стоит (0 байт через прокси); законный шаг, ждущий между запросами дольше ожидания блокировки, — не обрывается; шаг не короче потолка простоя раннера — отказ `register` |
| чужие парсеры драйвера | `saga-driver-parsers.db`, `decode-timestamp` | глобальный парсер timestamptz отдаёт строку или обёртку — экземпляр, журнал и срез читаются; разбор совпадает с парсером драйвера по умолчанию |
| поздний успех компенсации | `saga-remote.db` | `late-reply` фазы compensate у stuck; участник отвечает на дубль, ручной повтор доводит до `compensated` |
| «успех компенсации» у идущей саги текущего поколения | `saga-remote.db` | `stale`, журнал и статус не меняются |
| отсев чужих сообщений по полям | `saga-guards.db` | команда с чужим поколением, фазой, шагом, попыткой — каждое `stale`; ответ с чужим поколением, фазой — `stale` |
| защитные проверки | `saga-guards.db` | брошенная релеем доставка — без записи отказа; шаг, глухой к сигналу, — таймаут; укороченное определение той же версии — отказ; `lockTimeoutMs` в транзакции; отметка inbox по часам оркестратора |
| вид шага сменили | `saga-versioning.db` | удалённый → локальный без версии: ответ — `SagaDefinitionUnavailableError` и хук с причиной |
| текст отказа | `saga-error-text` | код и сообщение сервера из `cause`, без SQL и параметров; суррогатная пара не режется |
| отказ из другой копии пакета | `saga-step-rejected.error` | метка `Symbol.for`: окончательный отказ узнаётся без `instanceof` |
| поздний ответ при FIFO | `saga-remote.db` | `ordering: aggregate`: успех прежней попытки применён до отложенного повтора, участник получил одну команду |
| неизвестный исход без компенсации | `saga-transitions` | запись `uncompensated-uncertain`; у шага с компенсацией её нет |
| остаток бюджета шага | `saga-failures.db`, `saga-guards.db` | раннер с ожиданием строки 1 с, шаг 8 с из двух `pg_sleep(7.9)` (через `execute` и через построитель): `step-timed-out` записан тем же проходом релея, на сервере запроса нет; свой меньший потолок шага (50 мс) сохраняется — 57014 |
| сторож и `blocked` | `saga-stall-watch.db` | шаг обрывает своё соединение на каждой доставке до DEAD: один проход — `blocked`, `delivery-dead`, хук; повтор без порога доводит сагу; DEAD прошлой попытки и поколения не блокирует; компенсация в DEAD — повтор в фазе компенсации; таймер в DEAD; второй сторож под блокировкой видит `blocked`; `stalled` один раз на эпизод |
| схема 0.4.0 на таблицах 0.3.0 | `saga-upgrade-sql.db`, `saga-tables.db` | CHECK через `NOT VALID` + `VALIDATE` и индекс `CONCURRENTLY` на таблицах с данными; повторно без отказа; плохая строка — 23514 у `VALIDATE`; чужая команда без номера шага не роняет доводку |
| индекс замёрзших | `saga-store-plan.db` | 40 000 строк: `list` замёрзших и проход сторожа (и после курсора) идут индексом по последнему движению |
| текст отказа из любого значения | `saga-error-text`, `saga-value-text`, `saga-error-values.db` | объект без прототипа, сломанный `toString`, Proxy — текст, отказ записан с первой доставки; непарный суррогат — U+FFFD, ответ участника записан; выход шага с непарным суррогатом — отказ шага |
| неопределённость прежней попытки | `saga-remote.db` | `silent` → `reject`: компенсируется и `charge`; двойник `reject` сразу — нет |
| проверки входа | `saga-input-checks`, `saga-cancel.db`, `saga-admin.db` | NUL и непарный суррогат в id, вход null, Invalid Date (start, cancel, prune, list), пустая поправка; `cancel(at)` в прошлом не уводит отметку назад; CLI: пустой `--id`, неверные имена — код 2 до подключения; dry-run над неподходящим — код 1 |
| потолок доставки релея | `saga-orchestrator-config` | шаг вместе с ожиданием строки и ROLLBACK не короче (по потолкам РАННЕРА) `outbox.config.deliveryTimeoutMs` — отказ `register` (и при потолке outbox 10 с); outbox без `config` — отказ; определение мимо построителя — проверено при `register`; совпавшие типы событий и та же версия второй раз — отказ; хук `definitionUnavailable` — только на отказ определения |

Мутации (снятая правка → прогон своих спек на настоящем Postgres → откат копией файла; считаются упавшие проверки):

| Инвариант | Мутация | Красных проверок |
|---|---|---|
| атомарность шага и записи о нём | следующая команда пишется мимо транзакции шага | 2 (откат снимает всё; исход COMMIT «откатилась») |
| `tx` брошенного шага закрыт | ограда `tx` не закрывается | 1 (свой раннер: эффект попытки лёг в базу) |
| опережение агрегата на outbox без зоны | параметр приводится к `timestamptz` | 2 (опережение и порядок у `naiveLocal`) |
| `wake_at` доводки — только из срока сообщения | без `known`; без фильтра срока; фильтр `true` | 1; 1; 1 |
| длительности не отрицательны | без нижней границы у `stuckForMs`; у `oldestActiveAgeMs` | 1; 1 |
| поправка контекста CLI | поправка не передаётся в `retry`; пустой объект принят | 1; 1 |
| свои имена индексов | имя не проверяется; совпадение ограничений журнала не проверяется; неизвестный ключ принят | 1; 1; 1 |
| обратный порядок компенсаций | поиск компенсации снизу вверх | 5 |
| идемпотентность сообщения (inbox) | обработка без отметки inbox | 2 (дубль ответа; одна команда двум репликам) |
| один исполнитель на экземпляр | `SELECT … FOR UPDATE` без `FOR UPDATE` | 1 (успех и таймер одной попытки) |
| сообщение ровно той попытки | попытка не сверяется | 1 (таймер прежней попытки) |
| неизвестный исход таймаута | шаг после таймаута не компенсируется | 3 |
| обрыв ≠ отказ шага | любой отказ транзакции записывается как отказ шага | 4 |
| шаги версии сверяются с экземпляром | сравнивается только число шагов | 1 |
| пауза повтора | повтор без `deliverAt` | 2 |
| `stuck` по исчерпании компенсации | компенсация «проскакивает» к следующей | 3 |
| команда участнику не держит команды саги (FIFO) | её агрегат по умолчанию — `saga` | 2 |
| команда участнику не держит других участников (FIFO) | агрегат — на экземпляр, а не на шаг; свой `aggregateType` `saga` принимается | 2; 1 |
| возврат не обгоняет списание при расхождении часов | команда участнику — по своим часам без учёта ждущего в агрегате | 2 |
| сообщение прежнего воплощения | сверка таймера/команды без id; ответа без id | 1; 3 |
| незаписываемая команда — отказ шага | отказ записи outbox уходит наружу; класс 22 не детерминированный | 6; 5 |
| контекст компенсации | пересечение `Ctx & Out` / `Ctx & Partial<Out>`; выход шага только `Record` | 1 каждая (tsc) |
| выход шага — объект после JSON | проверка формы после JSON снята | 2 |
| буквальный `\u0000` — не NUL | поиск NUL подстрокой | 4 |
| потолок релея — из outbox | копия 20 с | 1 |
| отмена удалённого шага без компенсации | нет `uncompensated-uncertain` | 2 |
| late-reply только неучтённого | без «шаг выполнен» / без «команда уходила» | 50 / 48 |
| время после блокировки строки | время до блокировки: cancel, отправка, ответ, таймер, retry, resolve | по 1–2 на каждое |
| отметка не раньше последнего сдвига | без зажима у retry; у resolve | 2; 1 |
| сроки — по своим часам | команда перехода по отметке; пауза от отметки; команда участнику и таймер от отметки; команда ручного повтора по отметке | 4; 1; 1; 1 |
| пороги простоя дольше int4 | порог — int4 | 2 |
| доводка 0.1.0 | без `wake_at`; без переноса таймера | 1; 1 |
| хук после отмены без компенсаций | нет уведомления | 2 |
| пустая причина повтора | пишется | 1 |
| отчёт по последнему движению | порядок по `updated_at` | 1 |
| время не от глобального парсера | разбор глобальным парсером | 4 |
| поколение отмены в int4 | проверки нет | 1 |
| отмена: невыполненный локальный шаг не компенсируется | компенсация с текущего шага | 6 |
| ответ сверяется по шагу | сверки шага нет | 1 |
| замёрзшие — только активные | без фильтра статусов | 1 |
| пауза повтора — `wake_at` | `wake_at` не пишется | 1 |
| отказ шага после отмены не записывается | `_recordFailure` без сверки ожидания | 1 |
| данные ответа компенсации не разбираются | разбираются | 1 |
| нет `eventType` — отказ шага | проверки нет | 1 |
| 57014 — таймаут шага | ветки нет | 1 |
| воплощение не из момента старта | воплощение = `now` старта | 3 |
| id 0.1.0 сверены с литералами | формула без воплощения как с ним; экземпляр 0.1.0 принимает любой id | 2; 4 |
| экземпляру 0.1.0 — ключ её формата | ключ формата 2 всегда | 1 |
| потолок простоя транзакции | раннер без потолка; потолок = ожиданию блокировки; `register` без сверки | 1; 2; 1 |
| предупреждение о чужом id | нет предупреждения | 1 |
| доводка 0.1.0: устаревший `wake_at`; зона писателя | не сбрасывается; зона не учитывается | 1; 4 |
| типы: дистрибутивное слияние; `data`/`contextPatch` — `object`; словарь | без дистрибутивности; `Record`; `keyof` словаря | 1 каждая (tsc) |
| Map/Set/Error в выходе | проверки нет | 8 |
| текст late-reply у resolved | resolved — «после отката» | 1 |
| голый `TypeError` — не окончательный | любой `TypeError` окончательный | 3 |
| `cancel` только у running | без проверки статуса | 5 |
| отказ связи при записи команды — не отказ шага (проводка) | любой отказ записи — отказ шага | 3 |
| поздний успех компенсации выполненного шага | факт компенсации по `step-completed` | 1 |
| `resolve` только пригодного; порог простоя ≥ 1 | без проверки пригодности; 0 разрешён | 1; 1 |
| конфигурация оркестратора | типы событий не сверяются; повторная регистрация версии; хук `definitionUnavailable` на любой отказ | 1; 1; 1 |
| уведомление отмены — своего поколения | поколение не сверяется | 1 |
| CLI не висит за блокировкой | без `lock_timeout` | 1 |
| потолок запроса — остаток бюджета (0.4.0) | ограда без бюджета; без тени сессии drizzle (только `execute`); свой меньший потолок шага не сохраняется | 2; 1 (построитель); 1 |
| бюджет доставки (0.4.0) | без ROLLBACK; ожидание строки оркестратора вместо раннера; `lockTimeoutMs` не проверяется | 3; 1; 5 |
| `blocked` по ожидаемому сообщению | id по поколению 0 и попытке 1; без перепроверки под блокировкой; `stalled` без памяти эпизодов; `retry` над `blocked` не пускает; фаза повтора по статусу | 2; 1; 1; 2; 1 |
| схема 0.4.0 | агрегат участнику без `? 'stepIndex'`; CHECK без `NOT VALID`; индекс без выражения | 1; 1; 3 |
| текст отказа | `String()` в `sagaErrorPart`; без `toWellFormed`; `sagaValueText` = `String()`; без проверки суррогата в выходе | 1 (db); 1 (db); 14; 2 |
| неопределённость прежней попытки | `uncertain: false` у ответа-отказа | 1 |
| мелкие проверки входа | по одной на каждую: зажим `cancel(at)`, даты, NUL/суррогат id, вход null, потолок prune, пустая поправка, теги и `isView`, дубли шагов, NaN паузы, version int4, cause у проверки outbox, `bind` ограды, CLI: годность dry-run, имена, пустой id | по 1–2 на каждую |

Не мутировалось отдельно: запись СОСТОЯНИЯ мимо транзакции — такая мутация зависает на собственной блокировке строки (UPDATE с другого соединения ждёт `FOR UPDATE` своей же транзакции), то есть отличает сломанное от исправного таймаутом, а не утверждением; атомарность состояния закрывает та же проверка «откат снимает всё» (состояние, журнал, эффект и команда сверяются вместе).

## Лицензия

[MIT](LICENSE) © 2026 Hoshino Tatsuya
