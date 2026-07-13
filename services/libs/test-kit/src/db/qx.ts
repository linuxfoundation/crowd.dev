import { test as baseTest } from 'vitest'

import { pgpQx } from '@crowd/database'

import { openTestWorkerDatabase, resetTestDatabase, seedTestBaseline } from './postgres'

export function withQx<T extends typeof baseTest>(test: T) {
  return (
    test
      // eslint-disable-next-line no-empty-pattern
      .extend('_db', { scope: 'file' }, async ({}, { onCleanup }) => {
        const worker = await openTestWorkerDatabase()
        onCleanup(() => worker.cleanup())
        return worker.db
      })
      .extend('qx', async ({ _db }) => {
        const executor = pgpQx(_db)
        await resetTestDatabase(executor)
        await seedTestBaseline(executor)
        return executor
      })
  )
}
