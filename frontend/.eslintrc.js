module.exports = {
  root: true,

  env: {
    node: true,
    es2022: true,
  },

  extends: ['plugin:vue/vue3-recommended', '@vue/airbnb', '@vue/typescript', 'plugin:storybook/recommended'],

  parserOptions: {
    ecmaVersion: 2020,
    parser: '@typescript-eslint/parser',
  },

  rules: {
    'no-console':
      process.env.NODE_ENV === 'production'
        ? 'warn'
        : 'off',
    'no-debugger':
      process.env.NODE_ENV === 'production'
        ? 'warn'
        : 'off',
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
    'vue/no-v-html': 'off',
    'import/prefer-default-export': 'off',
    'import/no-named-as-default': 'off',
    'class-methods-use-this': 'off',
    'no-shadow': 'off',
    'vuejs-accessibility/mouse-events-have-key-events': 'off',
    'vuejs-accessibility/click-events-have-key-events': 'off',
    'vuejs-accessibility/form-control-has-label': 'off',
    'vuejs-accessibility/label-has-for': 'off',
    'func-names': 'off',
    'import/no-cycle': 'off',
    // TODO: This is a temporary rule to allow the use of destructuring in setup props - from package version upgrade
    'vue/no-setup-props-destructure': 'off',
    'vue/space-infix-ops': 'off',
    'vue/max-len': [
      'error',
      {
        code: 150,
        ignoreComments: true,
        ignoreUrls: true,
      },
    ],
    'import/extensions': [
      'error',
      'ignorePackages',
      {
        js: 'never',
        jsx: 'never',
        ts: 'never',
        tsx: 'never',
      },
    ],
  },

  settings: {
    'import/resolver': {
      alias: {
        extensions: ['.js', '.jsx', '.ts', '.tsx'],
        map: [
          ['@', './src'],
        ],
      },
      typescript: {},
    },
  },
};
