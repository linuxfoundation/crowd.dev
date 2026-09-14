module.exports = {
  singleQuote: true,
  arrowParens: 'always',
  printWidth: 100,
  trailingComma: 'all',
  semi: false,
  importOrder: [
    '^(?!(\\.|@crowd/|@/|\\.\\./))(.*)$', // 3rd-party imports
    '^@crowd/(.*)$', // crowd packages
    '^@/(.*)$', // local package absolute imports that start with @/
    '^\\.\\./', // relative imports that start with ../
    '^\\./', // same directory imports that start with ./
    '^\\.$', // current directory
  ],
  importOrderSeparation: true,
  importOrderSortSpecifiers: true,
  plugins: [require.resolve('@trivago/prettier-plugin-sort-imports')],
}
