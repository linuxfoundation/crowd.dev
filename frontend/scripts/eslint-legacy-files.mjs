// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

// Regenerates .eslint/legacy-files.json: the SFCs still violating each ratchet rule, exempted in .eslintrc.js.

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const frontendDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const eslintBin = path.join(frontendDir, 'node_modules', '.bin', 'eslint');
const outputFile = path.join(frontendDir, '.eslint', 'legacy-files.json');
const ESLINT_EXIT_CODE_LINT_ERRORS = 1;

const RULES = {
  'vue/component-api-style': ['error', ['script-setup']],
  'vue/block-lang': ['error', { script: { lang: 'ts' } }],
  'vue/prefer-define-options': 'error',
};

function findViolatingFiles(ruleName, ruleConfig) {
  const args = [
    'src/**/*.vue',
    '--no-eslintrc',
    '--parser',
    'vue-eslint-parser',
    '--parser-options',
    JSON.stringify({ parser: '@typescript-eslint/parser', ecmaVersion: 2022, sourceType: 'module' }),
    '--plugin',
    'vue',
    '--rule',
    JSON.stringify({ [ruleName]: ruleConfig }),
    '--format',
    'json',
  ];

  let stdout;
  try {
    stdout = execFileSync(eslintBin, args, { cwd: frontendDir, encoding: 'utf8', maxBuffer: 1024 * 1024 * 100 });
  } catch (err) {
    if (err.status !== ESLINT_EXIT_CODE_LINT_ERRORS) {
      throw err;
    }
    stdout = err.stdout;
  }

  let results;
  try {
    results = JSON.parse(stdout);
  } catch (err) {
    throw new Error(`ESLint returned no JSON report for ${ruleName}`, { cause: err });
  }

  return results
    .filter((result) => result.errorCount > 0)
    .map((result) => path.relative(frontendDir, result.filePath))
    .sort();
}

const legacy = {};
for (const [rule, config] of Object.entries(RULES)) {
  legacy[rule] = findViolatingFiles(rule, config);
}

fs.writeFileSync(outputFile, `${JSON.stringify(legacy, null, 2)}\n`);

for (const [rule, files] of Object.entries(legacy)) {
  console.log(`${rule}: ${files.length}`);
}
