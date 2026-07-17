import { loadEnv } from 'vite'
import { defineConfig } from 'vitest/config'

const root = __dirname

/** Load .env.${mode} and let existing process env vars override file values. */
function resolveTestEnv(mode: string): Record<string, string> {
  const env = loadEnv(mode, root, '')
  const resolved = { ...env }

  for (const key in env) {
    const value = process.env[key]

    if (value !== undefined) {
      resolved[key] = value
    }
  }

  return resolved
}

export default defineConfig(({ mode }) => ({
  test: {
    env: resolveTestEnv(mode),
    server: {
      deps: { inline: [/@crowd\//] },
    },
    projects: [
      {
        extends: true,
        test: {
          name: 'server',
          include: ['backend/**/*.test.ts', 'services/**/*.test.ts'],
          exclude: [
            '**/node_modules/**',
            '**/dist/**',
            'services/cronjobs/**',
            // TODO: packages_worker has its own vitest config and packages-db; it was landed in another PR
            // alongside the test foundation. excluding this for now, will refactor it later!
            'services/apps/packages_worker/**',
          ],
          pool: 'forks',
          hookTimeout: 90_000,
          testTimeout: 30_000,
        },
      },
    ],
  },
}))
