// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

// Regenerates .eslint/legacy-files.json: the SFCs still violating each ratchet rule, exempted in eslint.config.mjs.

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const frontendDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const eslintBin = path.join(frontendDir, 'node_modules', '.bin', 'eslint');
const outputFile = path.join(frontendDir, '.eslint', 'legacy-files.json');
const ESLINT_EXIT_CODE_LINT_ERRORS = 1;

const VUE = { glob: 'src/**/*.vue', ext: '.vue', plugin: 'vue' };

const RULES = {
  'vue/component-api-style': { ...VUE, config: ['error', ['script-setup']] },
  'vue/block-lang': { ...VUE, config: ['error', { script: { lang: 'ts' } }] },
  'vue/prefer-define-options': { ...VUE, config: 'error' },
  '@typescript-eslint/no-explicit-any': {
    glob: 'src/**/*.{ts,vue}',
    ext: '.ts,.vue',
    plugin: '@typescript-eslint',
    config: 'error',
  },
};

function findViolatingFiles(ruleName, { glob, ext, plugin, config: ruleConfig }) {
  const args = [
    glob,
    '--no-config-lookup',
    '--ext',
    ext,
    '--parser',
    'vue-eslint-parser',
    '--parser-options',
    JSON.stringify({ parser: '@typescript-eslint/parser', ecmaVersion: 2022, sourceType: 'module' }),
    '--plugin',
    plugin,
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
    .filter((result) => result.messages.some((message) => message.ruleId === ruleName))
    .map((result) => path.relative(frontendDir, result.filePath))
    .sort();
}

const legacy = {};
for (const [rule, spec] of Object.entries(RULES)) {
  legacy[rule] = findViolatingFiles(rule, spec);
}

fs.writeFileSync(outputFile, `${JSON.stringify(legacy, null, 2)}\n`);

for (const [rule, files] of Object.entries(legacy)) {
  console.log(`${rule}: ${files.length}`);
}
