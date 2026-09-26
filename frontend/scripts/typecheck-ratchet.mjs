// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

// Fails CI when vue-tsc errors exceed typecheck-baseline.json; each B ticket lowers the baseline.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const frontendDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const result = spawnSync(
  path.join(frontendDir, 'node_modules/.bin/vue-tsc'),
  ['--noEmit', '-p', 'tsconfig.json'],
  { cwd: frontendDir, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 },
);

const { stdout } = result;
if (result.error || ![0, 2].includes(result.status)) {
  console.error(`vue-tsc crashed (exit ${result.status}):`);
  console.error(result.error?.message ?? `${stdout}${result.stderr}`);
  process.exit(1);
}

const errorLines = stdout.split('\n').filter((line) => /\berror TS\d+:/.test(line));
// Config-level errors have no source location or point at tsconfig.json; the check did not really run.
const configErrors = errorLines.filter((line) => /^error TS|^tsconfig\.json\(/.test(line));
if (configErrors.length) {
  console.error(`tsconfig/project errors, type check did not run:\n${configErrors.join('\n')}`);
  process.exit(1);
}

const count = errorLines.length;
const baseline = JSON.parse(readFileSync(path.join(frontendDir, 'typecheck-baseline.json'), 'utf8')).errors;
if (!Number.isInteger(baseline)) {
  console.error('typecheck-baseline.json must be { "errors": <integer> }.');
  process.exit(1);
}

if (count > baseline) {
  console.log(errorLines.join('\n'));
  console.error(
    `Type errors rose from ${baseline} to ${count}. Fix them or, if pre-existing, raise the baseline with a justification.`,
  );
  process.exit(1);
}

if (count < baseline) {
  console.log(
    `Type errors dropped from ${baseline} to ${count}: lower the baseline to ${count} in typecheck-baseline.json.`,
  );
} else {
  console.log(`Type errors: ${count} (baseline ${baseline})`);
}
