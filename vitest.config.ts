import { loadEnv } from 'vite'
import { defineConfig } from 'vitest/config'

const root = __dirname

const testEnv = loadEnv('test', root, '')

export default defineConfig({
  test: {
    env: testEnv,
    server: {
      deps: { inline: [/@crowd\//] },
    },
    projects: [
      {
        extends: true,
        test: {
          name: 'server',
          include: ['backend/**/*.test.ts', 'services/**/*.test.ts'],
          exclude: ['**/node_modules/**', '**/dist/**', 'services/cronjobs/**'],
          pool: 'forks',
          hookTimeout: 300_000,
          testTimeout: 30_000,
        },
      },
    ],
  },
})
