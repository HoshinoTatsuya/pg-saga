# Changelog

All notable changes to this package are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the package uses
[Semantic Versioning](https://semver.org/) (before 1.0 a breaking change bumps the minor version).

## 0.4.1 — 2026-11-21

First public release as `@hoshinotatsuya/pg-saga`, MIT; documentation in English and Russian; no functional
changes.

- The cross-copy mark of `SagaStepRejectedError` is now `Symbol.for('@hoshinotatsuya/pg-saga:step-rejected')`
  (it carries the package name). Copies of this package published under an earlier name do not recognise each
  other's rejections by the mark; `instanceof` within one copy is unaffected.
- README examples live in `examples/` and are type-checked against the sources (`pnpm typecheck`).
- CI moved to GitHub Actions; development dependencies bumped (arch-gates 0.12.2, jest-groups 0.7.2,
  fault-kit 0.3.1).

## 0.4.0 — 2026-11-21

Fixes from a verified external review. API and behavior change in several places — see *Migration*.

### Changed

- **The SQL ceiling of a local step is the remaining step budget.** Before each statement of the step
  transaction `statement_timeout` is set to what is left of `timeoutMs` (a smaller ceiling set by the step is
  kept). Previously a late statement ran the whole step ceiling, the ROLLBACK queued behind it and the failure
  record missed the instance row, so the step re-ran on every redelivery until DEAD. `pg_cancel_backend` is not
  used (unsafe behind a transaction-mode pooler).
- **The delivery budget in `register` is `timeoutMs + lockTimeoutMs + rollbackTimeoutMs`, taken from the
  runner.** `sagaTransactionRunner` registers its own ceilings (new option `rollbackTimeoutMs`, default 2 s).
  `deps.lockTimeoutMs` and the runner options must be integers ≥ 1.
- The stall watch walks idle instances in a circle (cursor), and an episode counts as reported only when the
  hook did not throw.
- Saga and step names with lone surrogates are rejected at definition time.

### Added

- **`blocked` status and `SagaStallWatch`**: an instance whose expected message went DEAD in the outbox is moved
  to `blocked` with a `delivery-dead` journal entry and the `blocked` hook; `stalled` hook for idle instances.
  `SagaAdmin.retry`/`resolve` work on `blocked` without an idle threshold; `stuckReport` shows `stuck` and
  `blocked`; `stats` has `blocked`; `manualRetry` has `from`; `SagaAdmin.eligible`.
- **Schema**: CHECK constraints on instances (status, `step_index >= 0`, `attempt >= 1`, `generation >= 0`) and
  journal (phase), index `(status, last move, id)` for stalled lookups (`SagaIndexNames`: `instancesMoveIdx`,
  `instancesStateCheck`, `journalPhaseCheck`). `sagaUpgradeSql` upgrades existing tables (`NOT VALID` +
  `VALIDATE`, `CREATE INDEX CONCURRENTLY IF NOT EXISTS`); its `journal` option is now required.

### Fixed

- Failure text never throws (null-prototype objects, broken `toString`, Proxy, cross-realm errors) and is valid
  for jsonb (lone surrogates → U+FFFD); a step output with a lone surrogate is a final step failure.
- A remote step failing after an attempt with an unknown outcome compensates the step itself.
- Input checks: `cancel(at)`, `start` id/input/`now`, `prune(before)`, `list({ updatedBefore })`, prune batch
  ≤ 10 000, duplicate step names outside the builder, NaN from `random()`, more non-JSON types rejected in step
  outputs, empty context patches.
- CLI: an empty `--id` and invalid table/schema/type names exit with code 2 before connecting; a dry run on an
  ineligible instance exits with code 1 and the reason.

### Migration

1. Run the statements of `sagaUpgradeSql({ instances, journal, outbox, … })` **one by one, outside a
   transaction**; a row with an invalid status or numbers fails `VALIDATE` (23514) — fix it and rerun. The
   drizzle schema (`defineSagaTables`) got two CHECKs and an index.
2. A local step with `timeoutMs` ≥ 13 000 and the default outbox (20 s) is now rejected by `register`: shorten
   the step or raise the outbox `deliveryTimeoutMs`. With your own `deps.transaction`, set `deps.lockTimeoutMs`.
3. A step that sets a `statement_timeout` larger than the remaining budget no longer gets it.
4. Exhaustive `switch` over `SagaStatus` must handle `blocked`.
5. `start` with `null` input and `retry` with an empty patch are now rejected.
6. Schedule `SagaStallWatch.runOnce` if you want `blocked` and `stalled`.

## 0.3.0 — 2026-11-17

### Changed

- **A local step's `tx` is closed together with the step.** A step abandoned by timeout (deaf to `signal`) could
  still write through `tx`, and with a custom `deps.transaction` without connection fencing the write landed in a
  pooled connection next to the retry's effect. Now any call after the step ended fails before the connection.
- `sagaUpgradeSql` derives `wake_at` only from the deadline the message carries.
- `stuckForMs` and `oldestActiveAgeMs` are never negative.
- Peer ranges: outbox `^0.6.0 || ^0.7.0`, pg-runtime `^0.7.1 || ^0.8.0`.

### Added

- CLI `saga-stuck retry --context-patch <json>` / `--context-patch-file <path>`.
- `SagaTablesOptions.indexNames` (`SagaIndexNames`) — custom index and constraint names.

### Migration

Step code that uses `tx` after the step ended (a promise without `await`, work after a timeout) now gets an
error; such a query never reached the step transaction before either.

## 0.2.1 — 2026-11-17

### Changed

- Peer `pg-runtime` is `^0.7.1` (was `^0.6.0`), so the package installs with npm next to siblings that require
  pg-runtime 0.7 without `--legacy-peer-deps`. No API change.

## 0.2.0 — 2026-11-17

Breaking changes relative to 0.1.0.

### Rollout from 0.1.0

1. Add the column first: `ALTER TABLE saga_instances ADD COLUMN wake_at timestamp with time zone;`.
2. Upgrade remote-step participants **before** the orchestrator (`parseSagaCommand` ≥ 0.2.0 understands the step
   key with the incarnation). Instances started by 0.1.0 keep receiving keys and ids in the 0.1.0 format.
3. Upgrade all orchestrator replicas; keep the outbox `deadAfterMs` longer than the rollout.
4. Use only admin/CLI ≥ 0.2.0, also during the rollout.
5. When no 0.1.0 replica is left, run `sagaUpgradeSql` statement by statement, each in its own transaction.

### Changed

- `deps.outbox` must be the store with `config`; `register` checks local steps against
  `config.deliveryTimeoutMs`; saga event types are ≤ 64 chars and not coalesced.
- `sagaTransactionRunner` sets `idle_in_transaction_session_timeout` (default 20 s).
- `SagaAdmin.retry` requires `commandEventType`, CLI `retry` requires `--command-type`.
- A local step without `timeoutMs` gets 12 s, and `timeoutMs` also bounds the step's SQL.
- Random incarnation per instance, part of message ids and step keys (`<sagaId>#<incarnation>:<phase>:<index>`).
- Outbox aggregates: participant commands go to `saga-remote:<step index>`, replies to `saga-reply`, timers and
  deferred commands to their own (`SAGA_AGGREGATE_TYPE`).
- `SagaNotFoundError` removed: a message for a missing instance is `stale` with a warning.
- A step output whose JSON is not an object, or that contains Map/Set/WeakMap/WeakSet/Promise/Error, is a final
  step failure.
- Types: input, output and context accept any object type; `SagaContextMerge` and `SagaUncertainMerge`.
- Default `lockTimeoutMs` 10 → 5 s.

### Added

- `cancel`, `stalledReport`, `retry`/`resolve` with `stalledForMs`, `contextPatch`, `sagaUpgradeSql`,
  `SAGA_AGGREGATE_TYPE`, journal entries `uncompensated-uncertain` and `saga-cancelled`,
  `SagaStats.longestIdleActiveMs` and `resolved`.

### Fixed

- Deterministic failures (swallowed SQL error `25P02`, unwritable commands, unstorable reply data, slow steps,
  definitions built outside the builder) no longer freeze an instance forever; a participant that is down no
  longer holds FIFO for the saga or other participants; the same id after `prune` works; transition time is
  taken after the row lock; a frozen replica network no longer holds the instance row longer than the delivery
  ceiling; failure text without query parameters; time parsing independent of global driver parsers.

## 0.1.0 — 2026-11-15

First version: saga definitions with a typed context, local and remote steps, retries and timeouts,
compensations, `stuck` and manual repair (API and CLI), definition versions, metrics hooks and snapshots.
