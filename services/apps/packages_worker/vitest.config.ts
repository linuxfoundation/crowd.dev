import { defineConfig } from 'vitest/config'

const shared = {
  environment: 'node' as const,
  server: {
    deps: {
      inline: [/@crowd\//],
    },
  },
}

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          ...shared,
          name: 'packagist',
          include: ['src/packagist/**/*.test.ts'],
          // getPackagesDb() builds a lazy pg-promise pool from these at activity call
          // time; the packagist suites mock all queries so nothing ever connects.
          // Scoped to this project so DB-gated integration suites elsewhere (which
          // auto-skip when the real vars are absent) are unaffected.
          env: {
            CROWD_PACKAGES_DB_WRITE_HOST: '127.0.0.1',
            CROWD_PACKAGES_DB_PORT: '5432',
            CROWD_PACKAGES_DB_DATABASE: 'packages-test',
            CROWD_PACKAGES_DB_USERNAME: 'test',
            CROWD_PACKAGES_DB_PASSWORD: 'test',
          },
        },
      },
      {
        test: {
          ...shared,
          name: 'default',
          include: ['src/**/*.test.ts'],
          exclude: ['src/packagist/**'],
        },
      },
    ],
  },
})
