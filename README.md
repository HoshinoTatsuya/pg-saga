# @hoshinotatsuya/pg-saga

[![npm](https://img.shields.io/npm/v/@hoshinotatsuya/pg-saga.svg)](https://www.npmjs.com/package/@hoshinotatsuya/pg-saga)
[![CI](https://github.com/HoshinoTatsuya/pg-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/HoshinoTatsuya/pg-saga/actions/workflows/ci.yml)

[Русская версия](README.ru.md) · [Changelog](CHANGELOG.md)

Saga orchestration on plain PostgreSQL, on top of the transactional outbox of
[`@hoshinotatsuya/outbox`](https://www.npmjs.com/package/@hoshinotatsuya/outbox). No external cluster, no new
service: a saga is two tables in your database, its steps are outbox commands (at least once), replies go
through an inbox (applied once), and the instance state is written in **the same transaction** as the step's
own writes and the next command.

**The problem it solves.** A business operation spans several steps (reserve stock, charge a card, ship), some
of them in other systems. Any step can fail, time out or be retried, and the finished steps must be undone in
reverse order. Doing this by hand means a state machine, timers, retries, idempotency keys, crash recovery and a
way for a human to repair what got stuck. This package gives you all of that with the guarantees of a single
PostgreSQL transaction per transition.

| What | How |
|---|---|
| definition | `defineSaga<Input>(name, { version }).step(…).remoteStep(…).build()`; a step's output is merged into the context of the next steps, and reading a field no step provides does not compile |
| atomicity | one transaction per message: inbox mark → `SELECT … FOR UPDATE` of the instance → step → state, journal and next command in the outbox. Rolled back means no step effect, no record of it and no command |
| local step | `execute`/`compensate` receive the step transaction (`tx`): their database writes commit together with the saga's progress |
| remote step | a command to a participant through the outbox plus a reply timer (`deliverAt`); the reply comes back via `handleReply` or a `saga.reply` outbox event |
| retry | per step: attempts, pause `min(cap, base × 2^(n−1))` with half of it random; `SagaStepRejectedError` (or `retryable: false` in a reply) means no retries |
| timeout | local: `timeoutMs` (default 12 s) aborts the wait **and** the step's SQL (each statement gets `statement_timeout` = the remaining step budget); remote: a timer event, after which the outcome is **unknown** |
| compensation | finished steps that have a compensation, strictly in reverse order; after a remote timeout the timed-out step too |
| failed compensation | retried (`compensationRetry`); when exhausted the instance becomes `stuck` and waits for a human (`SagaAdmin`, the `saga-stuck` CLI) |
| dead delivery | `SagaStallWatch`: when the message an instance waits for goes DEAD in the outbox, the instance becomes `blocked` and the `blocked` hook fires |
| concurrency | one executor per instance (row lock); a redelivery is `duplicate` (inbox), a message of another attempt/step/generation or of a previous instance with the same id is `stale` |
| versions | an instance finishes on the definition version it started with; a missing or diverged version is a delivery failure with a reason, the instance is not touched |
| observability | `SagaMetrics` hooks after commit, `SagaAdmin.stats` snapshot, reports of stuck and stalled instances, a journal table |

## Install

```sh
pnpm add @hoshinotatsuya/pg-saga @hoshinotatsuya/outbox @hoshinotatsuya/pg-runtime drizzle-orm pg
pnpm add -D @types/pg   # optional, for TypeScript
```

Peer dependencies (see `peerDependencies` in `package.json` for the exact ranges):

| Peer | Range | Why |
|---|---|---|
| `@hoshinotatsuya/outbox` | `^0.6.0 \|\| ^0.7.0 \|\| ^0.8.0` | outbox store, inbox, relay, handlers |
| `@hoshinotatsuya/pg-runtime` | `^0.7.1 \|\| ^0.8.0 \|\| ^0.9.0` | `transactionKeepingFailure` behind `sagaTransactionRunner` |
| `drizzle-orm` | `>=0.45.2 <0.46` | database handle (`drizzle(pool)`), table schema for drizzle-kit |
| `pg` | `^8.18.0` | driver |
| `@types/pg` | `>=8.15.0` (optional) | types |

The package is CommonJS with ESM type declarations (`.d.mts`); it works with `commonjs`, `node16`, `nodenext`
and `bundler` module resolution (checked by `pnpm test:consumers`).

## Quick start

A saga with a local step (`reserve`, a row in your database) and a remote step (`charge`, a command to a
billing participant). The tables come from a migration you own.

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

The participant of the remote step is an ordinary outbox handler. It applies its effect once per `stepKey` and
replies with an outbox event in the same transaction:

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

Register it next to the orchestrator: `registerBilling(handlers, billingParticipant(db, outbox, inbox))`. A
participant in another system replies over its own transport, and your receiver calls
`orchestrator.handleReply(reply)`.

Every example in this README is a file in [`examples/`](examples) that is type-checked against the package
sources by `pnpm typecheck`.

## Concepts

- **Instance** — one run of a saga, a row in `saga_instances` keyed by your id (`order:42`). Statuses
  (`SAGA_STATUS`): `running`, `compensating`, `completed`, `compensated`, `stuck`, `resolved`, `blocked`.
- **Context** — a JSON object (`saga_instances.context`): the start input plus every step output. Each step gets
  a **frozen** copy; the only way to change the context is a step's return value.
- **Generation** — bumped by cancel and manual retry; commands and timers of an older generation are `stale`.
- **Incarnation** — a random 48-bit integer chosen at start; it is part of every message id and of the step key,
  so messages of an earlier instance with the same id (after `prune`) never apply to a new one.
- **Step key** — `<sagaId>#<incarnation>:<phase>:<step index>`, the same across all attempts and generations of
  an instance: pass it to external systems as the idempotency key. In the compensation phase `executeStepKey`
  is the key of the execution being undone.
- **Journal** — `saga_journal`, every event of an instance in order (`SAGA_JOURNAL_EVENT`).

## API

### `defineSaga<Input>(name, { version })` → `SagaBuilder`

`name` (and every step name) is a non-empty string of up to 64 characters (`SAGA_LIMIT.nameLength`) without
control characters or lone surrogates; `version` is an integer ≥ 1. The builder:

- `.step(name, spec)` — a **local step** (`SagaLocalStepSpec`):
  - `execute({ tx, context, sagaId, stepKey, executeStepKey, attempt, signal })` → `Promise<output object | void>`;
  - `compensate?(args)` — undo; without it the step is skipped while compensating;
  - `retry?` / `compensationRetry?` — `{ maxAttempts, backoffMs, maxBackoffMs }`, defaults
    `SAGA_DEFAULTS.stepRetry` = `{ 3, 1 000, 60 000 }` and `SAGA_DEFAULTS.compensationRetry` =
    `{ 5, 5 000, 300 000 }`;
  - `timeoutMs?` — default `SAGA_DEFAULTS.localStepTimeoutMs` = 12 000.
- `.remoteStep<Output>(name, spec)` — a **remote step** (`SagaRemoteStepSpec`):
  - `command({ context, sagaId, stepKey, executeStepKey, attempt })` → `{ eventType, payload, aggregateType?, headers? }`;
  - `compensation?(args)` — the undo command (its own `eventType`);
  - `timeoutMs` — **required**, the reply timer;
  - `retry?` / `compensationRetry?` as above.
- `.build()` → `SagaDefinition`. An empty saga, a duplicate step name or a remote step without `timeoutMs` throws
  `SagaConfigError`.

Context typing: output fields **override** same-named earlier fields (`SagaContextMerge`); the compensation of a
remote step sees `SagaUncertainMerge` (the output may not have arrived, so new fields are optional).

### `SagaOrchestrator`

```ts
new SagaOrchestrator(deps: SagaOrchestratorDeps)
```

| Option | Default | Meaning |
|---|---|---|
| `db` | — | drizzle database over a `pg` pool (`drizzle(pool)`) |
| `outbox` | — | `OutboxStore` of the same database; its `config.deliveryTimeoutMs` is the relay's delivery ceiling |
| `inbox` | — | `OutboxInbox` of the same database |
| `tables` | — | `{ instances, journal, schema? }` |
| `transaction` | `sagaTransactionRunner(db, { lockTimeoutMs })` | how the orchestrator opens its own transactions |
| `lockTimeoutMs` | 5 000 | lock wait ceiling in its own transactions (without `transaction`) |
| `consumer` | `'saga'` | inbox consumer name |
| `eventTypes` | `SAGA_EVENT_TYPE` (`saga.command`, `saga.timer`, `saga.reply`) | outbox event types of the saga, up to 64 chars, not coalesced |
| `metrics` | — | `SagaMetrics` hooks |
| `logger` | `console.warn` | `{ warn(message) }` |
| `clock` / `random` | `new Date()` / `Math.random` | for tests |

Methods:

- `register(definition)` → `this`. Validates the definition and **the delivery budget**: for every local step
  `timeoutMs + lockTimeoutMs + rollbackTimeoutMs` (of the runner that applies them; 12 + 5 + 2 = 19 s by
  default) must be below `outbox.config.deliveryTimeoutMs` (20 s by default), and shorter than the runner's
  idle-in-transaction ceiling. Several versions of one saga can be registered; the same version twice throws.
- `registerHandlers(handlers)` — registers the command, timer and reply handlers on an `OutboxHandlers`.
- `start(tx, definition, { id, input }, now?)` → `{ started, existing? }` — in **the caller's transaction**; a
  second start with the same id does nothing (`started: false`, `existing: { name, version, status }`).
- `cancel(tx, id, reason, at?)` → `{ cancelled, status }` — in the caller's transaction; only a `running`
  instance is cancelled (new generation, compensations of finished steps and of the current remote step).
- `handle(eventType, payload, signal?)` / `handleReply(payload)` → `SagaHandled`
  (`applied`, `failure-recorded`, `dispatched`, `duplicate`, `stale`) — used by the handlers or by your own
  transport.
- `eventTypes` — the effective event types (pass `eventTypes.command` to `SagaAdmin`).

### Participant helpers

- `parseSagaCommand(payload)` → `{ meta: SagaCommandMeta, data }` — throws `SagaPayloadError` on a malformed command.
- `sagaReplyOf(meta, { ok: true, data? } | { ok: false, error, retryable? })` → `SagaReply`.
- `sagaReplyMessage(reply, eventType = 'saga.reply')` → an outbox message for `outbox.enqueue`.
- `parseSagaReply(payload)` → `SagaReply` — validation used by `handleReply`.

### Schema

- `sagaTablesSql({ instances, journal, schema?, indexNames? })` → DDL string for your migration.
- `defineSagaTables(options)` → drizzle tables `{ instances, journal }` for drizzle-kit (the same tables; the spec
  compares them with the catalog).
- `sagaUpgradeSql({ instances, journal, outbox, schema?, outboxSchema?, eventTypes?, indexNames?, outboxNaiveTimeZone? })`
  → statements that bring tables of older versions up to date (see [CHANGELOG](CHANGELOG.md)). Run the
  statements one by one, outside a transaction (`CREATE INDEX CONCURRENTLY`).

Index and constraint names derive from the table name; a name longer than 63 bytes is rejected (PostgreSQL would
truncate it silently) — set `indexNames` then. Unknown keys, names outside `[a-z_][a-z0-9_]*` and collisions are
rejected before any DDL.

### `sagaTransactionRunner(db, options?)`

The orchestrator's default transaction: `transactionKeepingFailure` from `@hoshinotatsuya/pg-runtime` (the
connection never leaks, a broken COMMIT is `CommitOutcomeUnknownError`, retries on 40001/40P01) plus ceilings:

| Option | Default |
|---|---|
| `lockTimeoutMs` | 5 000 |
| `attempts` | 3 |
| `idleInTransactionTimeoutMs` | 20 000 |
| `rollbackTimeoutMs` | 2 000 |

Non-integer or < 1 values throw `SagaConfigError`.

### `SagaAdmin` — manual repair

```ts
new SagaAdmin({ db, transaction, tables, outbox?, commandEventType?, clock?, metrics? })
```

`show(id)`, `list(filter)`, `stats(since)`, `stuckReport(limit?)`, `stalledReport(olderThanMs, limit?)`,
`retry(id, note?, { stalledForMs?, contextPatch? })` (needs `outbox` and `commandEventType`),
`resolve(id, note, { stalledForMs? })`, `prune(before, limit = 5 000)`, static `eligible(instance, now, stalledForMs?)`.
A stalled active instance is retried or resolved **only** with an explicit `stalledForMs` threshold, otherwise a
live long step would run twice.

### `SagaStallWatch`

```ts
new SagaStallWatch({ db, transaction, outbox, tables, stalledAfterMs, blockAfterMs?, batchSize?, metrics?, logger?, clock? })
```

`runOnce()` → `{ checked, blocked, stalled }`: looks up, in the outbox dead letters, exactly the message each idle
instance waits for; found → `blocked` with a `delivery-dead` journal entry and the `blocked` hook; idle longer than
`stalledAfterMs` without a dead letter → the `stalled` hook (once per episode per process). Run it on a schedule
on one or two replicas.

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

### CLI `saga-stuck`

```sh
SAGA_DATABASE_URL=postgres://… npx saga-stuck list --instances saga_instances --journal saga_journal
npx saga-stuck retry --instances … --journal … --outbox app_outbox --command-type saga.command --id order:42 --note "fixed" --yes
```

Commands: `list | show | retry | resolve | stats | prune`; `--help` prints the full usage. `retry`, `resolve` and
`prune` change nothing without `--yes`. Exit codes: 0 — done, 1 — refused (e.g. the instance row is locked by a
running step: the CLI transaction has `lock_timeout` 5 s), 2 — invalid invocation (checked before connecting).

### Errors

| Error | When |
|---|---|
| `SagaConfigError` | invalid definition, options, budget or names — at definition, construction or `register` |
| `SagaPayloadError` | a malformed command or reply, an event the outbox will never accept |
| `SagaStateError` | `SagaAdmin.retry`/`resolve` of an instance that is not eligible (e.g. a live `running` one without `stalledForMs`) |
| `SagaDefinitionUnavailableError` | the instance's version is not registered in this process or its steps diverged |
| `SagaStepRejectedError` | throw it from a step to fail **without retries** (recognised anywhere in the `cause` chain and across package copies) |
| `SagaStepTimeoutError` | a local step exceeded `timeoutMs` |

### Constants

`SAGA_STATUS`, `SAGA_PHASE`, `SAGA_JOURNAL_EVENT`, `SAGA_HANDLED`, `SAGA_REPLY_OUTCOME`, `SAGA_EVENT_TYPE`,
`SAGA_AGGREGATE_TYPE`, `SAGA_DEFAULTS`, `SAGA_LIMIT` — all deep-frozen.

## Guarantees and limits

Guaranteed:

- **A local step is applied exactly once by its database effect**: its writes through `tx` commit with the
  "step done" mark and the next command, or not at all. Side effects outside the database (HTTP, e-mail) are at
  least once — use `stepKey` as the idempotency key.
- **Each message is applied at most once** (inbox); a concurrent redelivery is `duplicate`.
- **One executor per instance** (`FOR UPDATE` on the instance row); two replicas never advance one instance at
  the same time.
- **Compensations run in reverse order**, and an exhausted compensation stops the instance in `stuck` — it never
  silently skips to the next one.
- **Infrastructure failures do not burn step attempts**: a lost connection, an unknown COMMIT outcome, a timeout
  waiting for the instance row or the relay's delivery ceiling leave no record and the outbox redelivers.
- **A command the outbox will never accept** (payload too large, NUL, BigInt, `eventType` longer than 64, …) is a
  final step failure, not an endless redelivery.
- **Abandoned steps cannot write**: after a step's timeout its `tx` is fenced, any later call fails before
  reaching a connection.
- **Works with outbox FIFO per aggregate** (`ordering: aggregate`): a command to a participant that is down does
  not hold the saga's own commands nor commands to other participants, and a refund never overtakes its charge.

Not provided:

- exactly-once outside the database;
- ordering between different instances;
- isolation of compensations from concurrent sagas on the same data (semantic locks are up to your steps);
- inbox cleanup (`inbox.prune` with a margin longer than the outbox's worst redelivery time);
- `DateStyle` other than ISO on the server;
- a separate schema for the outbox in the CLI (`--schema` covers both);
- a check that `deps.outbox` lives in the same database — it must.

This is **not** a durable-execution engine: there is no deterministic replay of code, no arbitrary timers
(only reply timeouts and retry pauses), no signals or child workflows. If you have dozens of steps, heavy
branching or month-long timers, use a durable-execution engine instead.

## Failure behavior

The package fails closed:

- an invalid definition, option, name or delivery budget is rejected at definition, construction or `register`,
  before anything runs;
- a step that throws rolls its transaction back; the failure is recorded by a second transaction (retry with
  pause, or compensation);
- a remote step whose reply timer fires has an **unknown** outcome: after the retries it is compensated too, so
  a participant's compensation must tolerate "nothing to undo";
- a message for an instance that no longer exists (pruned) is `stale` with a warning, not an endless redelivery;
- a hook (`SagaMetrics`) that throws is swallowed with a warning: observation never breaks a saga;
- `start` and `cancel` wait for the instance row in **your** transaction, whose ceilings the package does not
  set: use `SET LOCAL lock_timeout` (see `cancelOrder` above) and retry on `55P03`.

## Compatibility

- Node.js ≥ 24.
- PostgreSQL 15, 16, 17, 18 (the full suite runs on each); also behind PgBouncer in transaction mode.
- `drizzle-orm` 0.45.x, `pg` 8.18+, TypeScript 5.9+.

## Testing and development

```sh
pnpm install
pnpm typecheck        # sources, examples, and README examples verbatim
pnpm lint
pnpm test             # needs Docker: starts postgres:17-alpine on a random port
pnpm test:consumers   # packs the build and installs it into throwaway commonjs/node16/nodenext/ESM projects
pnpm build
```

`pnpm test` runs Jest through `jest-groups`: groups of at most 300 tests, one after another, at most 2 workers
each — the suite never saturates the machine. Set `PG_TEST_URL` to use an existing server (CI does) and
`PG_TEST_IMAGE` for another image (`postgres:15-alpine` … `postgres:18-alpine`). Each suite runs under a role
**without** superuser rights, with a real outbox relay driven by a controlled clock.

## License

[MIT](LICENSE) © 2026 Hoshino Tatsuya
