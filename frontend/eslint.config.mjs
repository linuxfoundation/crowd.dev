// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { vueTsConfigs, withVueTs } from '@vue/eslint-config-typescript';
import prettier from 'eslint-config-prettier';
import { createTypeScriptImportResolver } from 'eslint-import-resolver-typescript';
import importX, { createNodeResolver } from 'eslint-plugin-import-x';
import storybook from 'eslint-plugin-storybook';
import pluginVue from 'eslint-plugin-vue';
import globals from 'globals';
import tseslint from 'typescript-eslint';

// Values frozen from @vue/eslint-config-airbnb 7.0.0, which has no ESLint 9+ release.
import airbnb from './.eslint/airbnb-rules.json' with { type: 'json' };
import legacy from './.eslint/legacy-files.json' with { type: 'json' };

const { fixable, ...noDuplicatesMeta } = importX.rules['no-duplicates'].meta;

// import-x 4.17.1's no-duplicates fixer merges value imports into `import type`, which erases them.
const importPlugin = {
  ...importX,
  rules: {
    ...importX.rules,
    'no-duplicates': {
      ...importX.rules['no-duplicates'],
      meta: noDuplicatesMeta,
      create: (context) =>
        importX.rules['no-duplicates'].create(
          Object.create(context, {
            report: { value: ({ fix, ...descriptor }) => context.report(descriptor) },
          }),
        ),
    },
  },
};

export default [
  ...(await withVueTs(
    {
      ignores: [
        '**/dist/**',
        '**/storybook-static/**',
        '**/coverage/**',
        '**/tailwind.config.js',
        '**/.tailwind/**',
        '**/components.d.ts',
      ],
    },
    // ESLint 9+ defaults this to 'warn', which --max-warnings=0 turns into failures.
    { linterOptions: { reportUnusedDisableDirectives: 'off' } },
    {
      files: ['**/*.{js,mjs,cjs,ts,vue}'],
      languageOptions: { globals: { ...globals.browser, ...globals.node } },
    },
    pluginVue.configs['flat/recommended'],
    {
      // Registered as `import` so existing `import/...` rule ids and disable directives keep working.
      plugins: { import: importPlugin },
      settings: {
        'import-x/extensions': ['.mjs', '.js', '.jsx'],
        'import-x/ignore': ['node_modules', '\\.(coffee|scss|css|less|hbs|svg|json)$'],
        'import-x/resolver-next': [
          createTypeScriptImportResolver({ project: `${import.meta.dirname}/tsconfig.json` }),
          createNodeResolver({ extensions: ['.mjs', '.js', '.json'] }),
        ],
      },
      rules: airbnb,
    },
    vueTsConfigs.base,
    tseslint.configs.eslintRecommended,
    // Plain-JS script blocks go through the TypeScript parser too.
    { files: ['**/*.vue'], languageOptions: { parserOptions: { parser: tseslint.parser } } },
    {
      files: ['**/*.{ts,tsx,vue}'],
      rules: {
        'no-undef': 'off',
        '@typescript-eslint/no-unused-vars': ['warn', { caughtErrors: 'none' }],
      },
    },
    // Reproduces typescript-eslint 5's eslint-recommended override list for .ts files.
    {
      files: ['**/*.ts'],
      rules: { 'no-class-assign': 'error', 'no-with': 'error', 'valid-typeof': 'off' },
    },
    ...storybook.configs['flat/recommended'],
    { files: ['**/*.{js,mjs,cjs,ts}'], rules: prettier.rules },
    {
      rules: {
        'no-console': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
        'no-debugger': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
        semi: 'warn',
        'comma-dangle': 'warn',
        indent: 'warn',
        'no-trailing-spaces': 'warn',
        'vue/no-unused-components': 'error',
        'vue/html-closing-bracket-spacing': 'warn',
        'vue/html-indent': 'warn',
        'vue/html-self-closing': 'warn',
        'object-curly-spacing': 'warn',
        'vue/html-button-has-type': 'warn',
        'import/order': 'warn',
        'keyword-spacing': 'warn',
        'space-before-blocks': 'warn',
        quotes: 'warn',
        'no-unused-vars': 'off',
        'no-multiple-empty-lines': 'warn',
        // ESLint 10 changed the defaults of these four rules; pinned to the ESLint 8 behaviour.
        'no-constant-condition': ['warn', { checkLoops: 'all' }],
        'no-inner-declarations': ['error', 'functions', { blockScopedFunctions: 'disallow' }],
        'no-shadow-restricted-names': ['error', { reportGlobalThis: false }],
        'no-useless-computed-key': ['error', { enforceForClassMembers: false }],
        'vue/no-v-html': 'off',
        // New in eslint-plugin-vue 10's recommended preset; off to keep the enabled rule set unchanged.
        'vue/no-required-prop-with-default': 'off',
        'import/prefer-default-export': 'off',
        'import/no-named-as-default': 'off',
        'class-methods-use-this': 'off',
        'no-shadow': 'off',
        'func-names': 'off',
        'import/no-cycle': 'off',
        'vue/space-infix-ops': 'off',
        'vue/component-api-style': ['error', ['script-setup']],
        'vue/block-lang': ['error', { script: { lang: 'ts' } }],
        'vue/prefer-define-options': 'error',
        'vue/define-macros-order': [
          'error',
          { order: ['defineOptions', 'defineProps', 'defineEmits', 'defineSlots'], defineExposeLast: true },
        ],
        'vue/max-len': ['error', { code: 150, ignoreComments: true, ignoreUrls: true }],
        'import/extensions': [
          'error',
          'ignorePackages',
          { js: 'never', jsx: 'never', ts: 'never', tsx: 'never' },
        ],
      },
    },
    {
      files: ['eslint.config.mjs', 'vite.config.ts', 'scripts/**'],
      rules: {
        'import/no-extraneous-dependencies': [
          'error',
          { devDependencies: true, optionalDependencies: false },
        ],
      },
    },
    ...Object.entries(legacy)
      .filter(([, files]) => files.length > 0)
      .map(([rule, files]) => ({ files, rules: { [rule]: 'off' } })),
  )),
  // Outside withVueTs: inside it, this rule switches on type-aware parsing for every .ts and TS .vue file.
  {
    rules: {
      '@typescript-eslint/consistent-type-imports': [
        'error',
        {
          prefer: 'type-imports',
          fixStyle: 'separate-type-imports',
          disallowTypeAnnotations: false,
        },
      ],
    },
  },
];
