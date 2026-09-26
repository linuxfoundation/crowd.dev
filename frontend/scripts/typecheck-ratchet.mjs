// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

// Fails CI when vue-tsc errors exceed typecheck-baseline.json; each B ticket lowers the baseline.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const frontendDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const baselineFile = path.join(frontendDir, 'typecheck-baseline.json');

const result = spawnSync(
  path.join(frontendDir, 'node_modules/.bin/vue-tsc'),
  ['--noEmit', '-p', 'tsconfig.json'],
  { cwd: frontendDir, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 },
);

const stdout = result.stdout ?? '';
if (result.error || (![0, 2].includes(result.status) && !stdout.trim())) {
  console.error('vue-tsc crashed:');
  console.error(result.error?.message ?? result.stderr);
  process.exit(1);
}

const errorLines = stdout.split('\n').filter((line) => /\berror TS\d+:/.test(line));
const count = errorLines.length;
const baseline = JSON.parse(readFileSync(baselineFile, 'utf8')).errors;

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
