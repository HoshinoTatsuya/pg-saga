#!/usr/bin/env node
import { pgExecutor, withPgTransaction } from '@hoshinotatsuya/outbox/pg';
import { Pool } from 'pg';

import { runSagaStuckCli } from './run-saga-stuck-cli';

void runSagaStuckCli(process.argv.slice(2), {
  env: process.env,
  stdout: (line) => process.stdout.write(`${line}\n`),
  stderr: (line) => process.stderr.write(`${line}\n`),
  connect: async (url) => {
    const pool = new Pool({ connectionString: url, max: 1 });
    return {
      executor: pgExecutor(pool),
      transaction: (work) => withPgTransaction(pool, work),
      close: () => pool.end(),
    };
  },
}).then((code) => {
  process.exitCode = code;
});
