import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import {
  listRubyGemsCriticalPackagesToSync,
  listRubyGemsPackagesToSync,
} from '@crowd/data-access-layer/src/osspckgs/rubygems'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getDbConnection } from '@crowd/database'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_PURL_PREFIX = 'pkg:gem/crowd-dev-test-rubygems-fixture-'

function daysAgo(days: number): Date {
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d
}

async function cleanupFixtures(qx: QueryExecutor): Promise<void> {
  await qx.result(
    `DELETE FROM versions WHERE package_id IN (SELECT id FROM packages WHERE purl LIKE $(prefix))`,
    { prefix: `${FIXTURE_PURL_PREFIX}%` },
  )
  await qx.result(`DELETE FROM packages WHERE purl LIKE $(prefix)`, {
    prefix: `${FIXTURE_PURL_PREFIX}%`,
  })
}

async function insertPackage(
  qx: QueryExecutor,
  name: string,
  overrides: {
    isCritical?: boolean
    ingestionSource?: string | null
    lastSyncedAt?: Date
  } = {},
): Promise<number> {
  const purl = `${FIXTURE_PURL_PREFIX}${name}`
  const row = await qx.selectOne(
    `INSERT INTO packages (purl, ecosystem, name, is_critical, ingestion_source, last_synced_at, created_at)
     VALUES ($(purl), 'rubygems', $(name), $(isCritical), $(ingestionSource), $(lastSyncedAt), NOW())
     RETURNING id`,
    {
      purl,
      name,
      isCritical: overrides.isCritical ?? false,
      ingestionSource: overrides.ingestionSource ?? null,
      lastSyncedAt: (overrides.lastSyncedAt ?? new Date()).toISOString(),
    },
  )
  return row.id as number
}

describe.skipIf(!HAVE_DB)('rubygems DAL — real packages-db', () => {
  let qx: QueryExecutor

  beforeAll(async () => {
    const conn = await getDbConnection({
      host: process.env.CROWD_PACKAGES_DB_WRITE_HOST ?? '',
      port: parseInt(process.env.CROWD_PACKAGES_DB_PORT ?? '0', 10),
      database: process.env.CROWD_PACKAGES_DB_DATABASE ?? '',
      user: process.env.CROWD_PACKAGES_DB_USERNAME ?? '',
      password: process.env.CROWD_PACKAGES_DB_PASSWORD ?? '',
    })
    qx = pgpQx(conn)
  }, 30_000)

  beforeEach(async () => {
    await cleanupFixtures(qx)
  })

  afterAll(async () => {
    if (qx) await cleanupFixtures(qx)
  })

  describe('listRubyGemsPackagesToSync', () => {
    it('returns never-synced and stale packages, excludes fresh ones', async () => {
      await insertPackage(qx, 'never-synced', { ingestionSource: null })
      await insertPackage(qx, 'fresh', {
        ingestionSource: 'rubygems-registry',
        lastSyncedAt: new Date(),
      })
      await insertPackage(qx, 'stale', {
        ingestionSource: 'rubygems-registry',
        lastSyncedAt: daysAgo(2),
      })

      const rows = await listRubyGemsPackagesToSync(qx, { limit: 100 })
      const names = rows.map((r) => r.name)

      expect(names).toContain('never-synced')
      expect(names).toContain('stale')
      expect(names).not.toContain('fresh')
    })
  })

  describe('listRubyGemsCriticalPackagesToSync', () => {
    it('returns is_critical rubygems packages and excludes non-critical', async () => {
      await insertPackage(qx, 'critical-a', { isCritical: true, ingestionSource: 'rubygems-registry' })
      await insertPackage(qx, 'critical-b', { isCritical: true, ingestionSource: null })
      await insertPackage(qx, 'non-critical', { isCritical: false, ingestionSource: null })

      const rows = await listRubyGemsCriticalPackagesToSync(qx, { limit: 100 })
      const names = rows.map((r) => r.name)

      expect(names).toContain('critical-a')
      expect(names).toContain('critical-b')
      expect(names).not.toContain('non-critical')
    })

    it('paginates by id cursor via afterId', async () => {
      const firstId = await insertPackage(qx, 'critical-cursor-1', { isCritical: true })
      await insertPackage(qx, 'critical-cursor-2', { isCritical: true })

      const rows = await listRubyGemsCriticalPackagesToSync(qx, { limit: 100, afterId: firstId })
      const ids = rows.map((r) => r.id)

      expect(ids.every((id) => id > firstId)).toBe(true)
      expect(rows.map((r) => r.name)).not.toContain('critical-cursor-1')
    })
  })
})
