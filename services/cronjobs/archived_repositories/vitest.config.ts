import { defineConfig } from 'vitest/config'

// This cronjob is outside the root workspace, and the root vitest config excludes it.
export default defineConfig({
  test: {
    root: __dirname,
    include: ['test/**/*.test.ts'],
  },
})
