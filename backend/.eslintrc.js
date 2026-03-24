module.exports = {
  env: {
    node: true,
    es2021: true,
  },
  extends: ['airbnb-base', 'prettier'],
  parser: '@typescript-eslint/parser',
  plugins: ['@typescript-eslint'],
  ignorePatterns: ['dist/*', '**/*.test.ts'],
  rules: {
    semi: ['error', 'never'],
    'prefer-destructuring': ['error', { object: false, array: false }],
    'no-param-reassign': 0,
    'no-underscore-dangle': 0,
    'no-unused-vars': [
      'error',
      { argsIgnorePattern: '^_', varsIgnorePattern: '^_', destructuredArrayIgnorePattern: '^_' },
    ],
    '@typescript-eslint/naming-convention': [
      'error',
      {
        selector: 'variableLike',
        leadingUnderscore: 'allow',
        trailingUnderscore: 'allow',
        format: ['camelCase', 'PascalCase', 'UPPER_CASE'],
      },
    ],
    'import/no-relative-packages': 0,
  },
  settings: {
    'import/resolver': {
      node: {
        paths: ['src'],
        extensions: ['.js', '.ts', '.d.ts', '.tsx'],
      },
    },
  },
  overrides: [
    {
      files: ['*.ts'],
      extends: ['airbnb-base', 'airbnb-typescript/base', 'prettier'],
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        tsconfigRootDir: __dirname,
        project: './tsconfig.json',
      },
      rules: {
        semi: ['error', 'never'],
        'prefer-destructuring': ['error', { object: false, array: false }],
        'no-param-reassign': 0,
        'no-underscore-dangle': 0,
        '@typescript-eslint/no-unused-vars': [
          'error',
          {
            argsIgnorePattern: '^_',
            varsIgnorePattern: '^_',
            destructuredArrayIgnorePattern: '^_',
          },
        ],
        '@typescript-eslint/naming-convention': [
          'error',
          {
            selector: 'variableLike',
            leadingUnderscore: 'allow',
            trailingUnderscore: 'allow',
            format: ['camelCase', 'PascalCase', 'UPPER_CASE'],
          },
        ],
        'import/no-relative-packages': 0,
        'global-require': 0,
        'import/prefer-default-export': 0,
        '@typescript-eslint/no-use-before-define': ['error', { functions: false, classes: false }],
        'no-restricted-syntax': 0,
        'no-plusplus': 0,
        'no-await-in-loop': 0,
        '@typescript-eslint/no-shadow': 0,
        'import/extensions': [
          'error',
          'never',
          {
            json: 'always',
          },
        ],
      },
    },
  ],
}
