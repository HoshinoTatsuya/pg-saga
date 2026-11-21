#!/usr/bin/env bash
# Матрица потребителей: собранный пакет (npm pack) ставится в одноразовые
# проекты с разными настройками модулей, и в каждом таблицы из
# `defineSagaTables` передаются СОБСТВЕННЫМ функциям drizzle потребителя
# (`getTableName`, `db.insert`). Проверяются tsc, ts-jest и загрузка входа в
# рантайме: декларации пакета для import (`.d.mts`) и require (`.d.ts`)
# резолвят drizzle по-разному, и расхождение видно только у потребителя.
#
# Отдельный случай — установка npm (он, в отличие от pnpm, отказывает на
# несовпадении peer) вместе с соседними пакетами семейства: peer pg-runtime
# `^0.6.0` не ставился рядом с project-scope 0.2.1 (`^0.7.1`) без
# `--legacy-peer-deps` — ERESOLVE.
#
# Запуск: scripts/consumer-matrix.sh. Каталоги — из mkdtemp, удаляются в конце.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pg-saga-consumers-XXXXXX")"
cleanup() {
  case "${WORK}" in
    "${TMPDIR:-/tmp}"/pg-saga-consumers-*) rm -rf "${WORK}" ;;
  esac
}
trap cleanup EXIT

(cd "${ROOT}" && pnpm build >/dev/null && npm pack --silent --pack-destination "${WORK}" >/dev/null)
TARBALL="$(ls "${WORK}"/hoshinotatsuya-pg-saga-*.tgz)"

CONSUMER_TS="import { getTableName } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { OutboxInbox, OutboxStore } from '@hoshinotatsuya/outbox';
import { defineSaga, defineSagaTables, SagaAdmin, SagaOrchestrator, sagaTransactionRunner, SAGA_STATUS } from '@hoshinotatsuya/pg-saga';

// skipLibCheck прячет ошибки в декларациях пакета, а нерезолвящийся импорт в них
// молча становится any: тогда таблица «принимается» чем угодно. Утверждение
// ниже не компилируется, если хоть один экспорт стал any.
type IsAny<T> = 0 extends 1 & T ? true : false;
type NotAny<T> = IsAny<T> extends true ? never : true;
export const typed: [
  NotAny<typeof SagaOrchestrator>, NotAny<typeof SagaAdmin>, NotAny<typeof defineSaga>,
  NotAny<typeof sagaTransactionRunner>, NotAny<ReturnType<typeof defineSagaTables>['instances']>,
] = [true, true, true, true, true];

export const tables = defineSagaTables({ instances: 'app_sagas', journal: 'app_saga_steps' });
export const tableName: string = getTableName(tables.instances);
export const insertRaw = (db: NodePgDatabase) =>
  db.insert(tables.instances).values({
    id: 's', name: 'n', version: 1, steps: ['a'], status: SAGA_STATUS.running, stepIndex: 0, attempt: 1,
    context: {}, createdAt: new Date(), updatedAt: new Date(),
  });
export const saga = defineSaga<{ orderId: string }>('order', { version: 1 })
  .step('reserve', { execute: async ({ context }) => ({ reservationId: context.orderId }) })
  .step('ship', { execute: async ({ context }) => ({ shipped: context.reservationId }) })
  .build();
// Вход — интерфейс; компенсация шага, меняющего тип поля, видит контекст после шага.
interface OrderInput {
  orderId: string;
  status: 'new';
}
export const retyped = defineSaga<OrderInput>('retyped', { version: 1 })
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
export const orchestrator = (db: NodePgDatabase) =>
  new SagaOrchestrator({
    db,
    outbox: new OutboxStore(db, { table: 'app_outbox' }),
    inbox: new OutboxInbox(db, { table: 'app_inbox' }),
    tables: { instances: 'app_sagas', journal: 'app_saga_steps' },
  }).register(saga);
export const startInTransaction = (db: NodePgDatabase) =>
  db.transaction((tx) => orchestrator(db).start(tx, saga, { id: 'o-1', input: { orderId: '1' } }));
"

SPEC_TS="import { tableName } from './consumer';

it('таблица библиотеки принимается функциями drizzle потребителя', () => {
  expect(tableName).toBe('app_sagas');
});
"

failures=0
run_case() {
  local label="$1" module="$2" resolution="$3" type="$4" jest="$5"
  local dir="${WORK}/${label}"
  mkdir -p "${dir}/src"
  cat >"${dir}/package.json" <<JSON
{
  "name": "consumer-${label}",
  "private": true,
  "type": "${type}",
  "dependencies": {
    "@hoshinotatsuya/pg-saga": "file:${TARBALL}",
    "@hoshinotatsuya/outbox": "0.8.1",
    "@hoshinotatsuya/pg-runtime": "0.9.1",
    "drizzle-orm": "0.45.2",
    "pg": "8.21.0"
  },
  "devDependencies": {
    "@types/jest": "30.0.0",
    "@types/node": "24.0.15",
    "@types/pg": "8.23.1",
    "jest": "30.4.2",
    "ts-jest": "29.4.11",
    "typescript": "6.0.3"
  }
}
JSON
  local resolution_line=""
  if [[ -n "${resolution}" ]]; then
    resolution_line="\"moduleResolution\": \"${resolution}\","
  fi
  cat >"${dir}/tsconfig.json" <<JSON
{
  "compilerOptions": {
    "module": "${module}",
    ${resolution_line}
    "target": "ES2024",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "types": ["jest", "node"],
    "noEmit": true
  },
  "include": ["src"]
}
JSON
  printf '%s' "${CONSUMER_TS}" >"${dir}/src/consumer.ts"
  # ESM с nodenext требует расширение в относительном импорте; CJS-раннер jest — без него.
  if [[ "${type}" == "module" && "${resolution}" == "nodenext" ]]; then
    printf '%s' "${SPEC_TS}" | sed "s#'./consumer'#'./consumer.js'#" >"${dir}/src/consumer.spec.ts"
  else
    printf '%s' "${SPEC_TS}" >"${dir}/src/consumer.spec.ts"
  fi
  cat >"${dir}/jest.config.js" <<'JS'
module.exports = {
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  transform: { '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json' }] },
  maxWorkers: 2,
};
JS
  (cd "${dir}" && pnpm install --prefer-offline --silent >"${dir}/install.log" 2>&1) || {
    echo "${label}: установка не прошла"; tail -5 "${dir}/install.log"; failures=$((failures + 1)); return
  }
  if (cd "${dir}" && node_modules/.bin/tsc -p tsconfig.json >"${dir}/tsc.log" 2>&1); then
    echo "${label}: tsc — зелёный"
  else
    echo "${label}: tsc — КРАСНЫЙ"; head -5 "${dir}/tsc.log"; failures=$((failures + 1))
  fi
  # Рантайм того же условия, которое выбирают декларации: import для ESM-проекта, require — для CJS.
  local runtime='const s = require("@hoshinotatsuya/pg-saga"); if (typeof s.SagaOrchestrator !== "function" || !Object.isFrozen(s.SAGA_STATUS)) process.exit(1);'
  if [[ "${type}" == "module" ]]; then
    runtime='const s = await import("@hoshinotatsuya/pg-saga"); if (typeof s.SagaOrchestrator !== "function" || !Object.isFrozen(s.SAGA_STATUS)) process.exit(1);'
  fi
  if (cd "${dir}" && node --input-type="$([[ "${type}" == "module" ]] && echo module || echo commonjs)" -e "${runtime}" >"${dir}/runtime.log" 2>&1); then
    echo "${label}: рантайм входов — зелёный"
  else
    echo "${label}: рантайм входов — КРАСНЫЙ"; head -5 "${dir}/runtime.log"; failures=$((failures + 1))
  fi
  if [[ "${jest}" == "jest" ]]; then
    if (cd "${dir}" && node_modules/.bin/jest --maxWorkers=2 >"${dir}/jest.log" 2>&1); then
      echo "${label}: ts-jest — зелёный ($(grep -E '^Tests:' "${dir}/jest.log"))"
    else
      echo "${label}: ts-jest — КРАСНЫЙ"; grep -E 'TS[0-9]+|Tests:' "${dir}/jest.log" | head -5; failures=$((failures + 1))
    fi
  fi
}

run_case commonjs commonjs "" commonjs jest
run_case node16 node16 node16 commonjs jest
run_case nodenext nodenext nodenext commonjs jest
run_case bundler-esm esnext bundler module tsc-only
run_case nodenext-esm nodenext nodenext module tsc-only

# npm без флагов ослабления peer: пакет ставится рядом с project-scope 0.2.1 и
# pg-runtime 0.7.1 на последних drizzle-orm/pg, и вход грузится в рантайме —
# со всеми поддерживаемыми линиями outbox (peer `^0.6.0 || ^0.7.0 || ^0.8.0`).
npm_family_case() {
  local outbox="$1" runtime="$2" neighbour="$3"
  local label="npm-family-outbox-${outbox}-runtime-${runtime}"
  local dir="${WORK}/${label}"
  mkdir -p "${dir}"
  cat >"${dir}/package.json" <<JSON
{
  "name": "consumer-${label}",
  "private": true,
  "dependencies": {
    "@hoshinotatsuya/pg-saga": "file:${TARBALL}",
    ${neighbour}
    "@hoshinotatsuya/pg-runtime": "${runtime}",
    "@hoshinotatsuya/outbox": "${outbox}",
    "drizzle-orm": "0.45.4",
    "pg": "8.23.1"
  }
}
JSON
  if (cd "${dir}" && npm install --legacy-peer-deps=false --strict-peer-deps=false --no-audit --no-fund >"${dir}/install.log" 2>&1); then
    echo "${label}: npm install без флагов — зелёный"
  else
    echo "${label}: npm install без флагов — КРАСНЫЙ"; grep -E 'ERESOLVE|peer|Could not resolve' "${dir}/install.log" | head -8
    failures=$((failures + 1)); return
  fi
  if (cd "${dir}" && node -e 'const s = require("@hoshinotatsuya/pg-saga"); const r = require("@hoshinotatsuya/pg-runtime/package.json"); if (typeof s.SagaOrchestrator !== "function" || r.version !== process.argv[1]) process.exit(1);' "${runtime}" >"${dir}/runtime.log" 2>&1); then
    echo "${label}: рантайм с pg-runtime ${runtime} — зелёный"
  else
    echo "${label}: рантайм с pg-runtime ${runtime} — КРАСНЫЙ"; head -5 "${dir}/runtime.log"; failures=$((failures + 1))
  fi
}
# Публично выпущены только последние линии (outbox 0.8, pg-runtime 0.9, project-scope 0.3):
# прежние (outbox 0.6/0.7, pg-runtime 0.7/0.8) на npmjs.com не публиковались, их совместимость
# проверена до публикации и в публичном CI не повторяется — ставить нечего.
npm_family_case 0.8.1 0.9.1 '"@hoshinotatsuya/project-scope": "0.3.2",'
npm_family_case 0.8.1 0.9.1 ''

if [[ "${failures}" -gt 0 ]]; then
  echo "матрица потребителей: красных ${failures}"
  exit 1
fi
echo "матрица потребителей: все зелёные"
