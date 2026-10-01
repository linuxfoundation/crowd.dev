// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const frontendDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const madgeBin = path.join(frontendDir, 'node_modules', '.bin', 'madge');
const allowlistFile = path.join(frontendDir, 'cycles-allowlist.json');
const MADGE_EXIT_CODE_CYCLES_OR_CRASH = 1;
const ARGS = '--circular --json --extensions js,ts,vue --ts-config tsconfig.json src'.split(' ');

function normalizeCycle(cycle) {
  const start = cycle.indexOf(cycle.toSorted()[0]);
  return [...cycle.slice(start), ...cycle.slice(0, start)].join(' > ');
}

function findCycles() {
  let stdout;
  try {
    stdout = execFileSync(madgeBin, ARGS, {
      cwd: frontendDir,
      encoding: 'utf8',
      maxBuffer: 1024 * 1024 * 100,
    });
  } catch (err) {
    if (err.status !== MADGE_EXIT_CODE_CYCLES_OR_CRASH) {
      throw err;
    }
    stdout = err.stdout;
  }

  let cycles;
  try {
    cycles = JSON.parse(stdout);
  } catch (err) {
    throw new Error(`madge returned no JSON report:\n${stdout}`, { cause: err });
  }
  if (!Array.isArray(cycles)) {
    throw new Error(`madge returned an unexpected report:\n${stdout}`);
  }

  return [...new Set(cycles.map(normalizeCycle))].sort();
}

function readAllowlist() {
  let previous;
  try {
    previous = JSON.parse(fs.readFileSync(allowlistFile, 'utf8'));
  } catch (err) {
    if (err.code === 'ENOENT') {
      return [];
    }
    if (!(err instanceof SyntaxError)) {
      throw err;
    }
  }
  if (!Array.isArray(previous)) {
    throw new Error(
      'cycles-allowlist.json is not a valid JSON array. Delete it and run `npm run lint:cycles:update`.',
    );
  }
  return previous;
}

function printGroup(heading, cycles) {
  if (cycles.length > 0) {
    console.error(`${heading} (${cycles.length}):`);
    cycles.forEach((cycle) => console.error(`  ${cycle}`));
  }
}

function hintFor(added) {
  if (added.length === 0) {
    return 'Run `npm run lint:cycles:update` and commit the result.';
  }
  return [
    'Break the new cycles instead of allow-listing them.',
    'If you only removed or moved imports, madge did not report these before. Run `npm run lint:cycles:update` and review the allowlist diff.',
  ].join('\n');
}

function main() {
  const args = process.argv.slice(2);
  const isUpdate = args.length === 1 && args[0] === '--update';
  if (args.length > 0 && !isUpdate) {
    console.error('usage: node scripts/check-cycles.mjs [--update]');
    process.exitCode = 2;
    return;
  }

  const current = findCycles();
  const previous = readAllowlist();
  const added = current.filter((cycle) => !previous.includes(cycle));
  const removed = previous.filter((cycle) => !current.includes(cycle));

  if (isUpdate) {
    fs.writeFileSync(allowlistFile, `${JSON.stringify(current, null, 2)}\n`);
    console.log(
      `wrote ${current.length} cycles to cycles-allowlist.json (+${added.length} -${removed.length})`,
    );
    return;
  }

  if (added.length === 0 && removed.length === 0) {
    console.log(`${current.length} known cycles, 0 new`);
    return;
  }

  printGroup('New circular imports, not in cycles-allowlist.json', added);
  printGroup('Stale cycles-allowlist.json entries, madge no longer reports the cycle', removed);
  console.error(hintFor(added));
  process.exitCode = 1;
}

main();
