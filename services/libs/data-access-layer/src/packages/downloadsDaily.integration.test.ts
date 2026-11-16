import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'
import {
  getNewestDownloadDate,
  getNpmPackagesNeedingDailyBackfill,
  upsertDailyDownloads,
} from './downloadsDaily'

// Integration test: hits the running packages-db. Skipped automatically when any of
// the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'npm-daily-downloads-fixture'
// Far enough back that only fixtures and never-processed packages are due.
const CUTOFF = '2001-01-10T03:30:00.000Z'

describe.skipIf(!HAVE_DB)('npm daily downloads: real packages-db', () => {
  let qx: QueryExecutor

  async function cleanupFixtures(): Promise<void> {
    const fixtures = `SELECT id FROM packages WHERE ingestion_source = $(tag)`
    await qx.result(`DELETE FROM downloads_daily WHERE package_id IN (${fixtures})`, {
      tag: FIXTURE_TAG,
    })
    await qx.result(
      `DELETE FROM npm_package_state WHERE purl IN (
         SELECT purl FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
  }

  async function makePackage(
    name: string,
    opts: { scope?: string; critical?: boolean; processedAt?: string } = {},
  ): Promise<{ id: string; purl: string }> {
    const purl = opts.scope ? `pkg:npm/%40${opts.scope}/${name}` : `pkg:npm/${name}`
    const row: { id: string } = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, namespace, name, registry_url, status, ingestion_source, is_critical)
       VALUES ($(purl), 'npm', $(namespace), $(name), 'https://example.test', 'active', $(tag), $(critical))
       RETURNING id::text AS id`,
      {
        purl,
        namespace: opts.scope ? `@${opts.scope}` : null,
        name,
        tag: FIXTURE_TAG,
        critical: opts.critical ?? true,
      },
    )
    if (opts.processedAt) {
      await qx.result(
        `INSERT INTO npm_package_state (purl, daily_downloads_last_processed_at)
         VALUES ($(purl), $(processedAt)::timestamptz)`,
        { purl, processedAt: opts.processedAt },
      )
    }
    return { id: row.id, purl }
  }

  async function storedCount(packageId: string, date: string): Promise<number> {
    const row = await qx.selectOne(
      `SELECT count FROM downloads_daily WHERE package_id = $(packageId)::bigint AND date = $(date)::date`,
      { packageId, date },
    )
    return Number(row.count)
  }

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
    await cleanupFixtures()
  })

  afterAll(async () => {
    if (qx) await cleanupFixtures()
  })

  describe('upsertDailyDownloads', () => {
    it('inserts days that are not stored yet', async () => {
      const { id } = await makePackage('insert-new')

      const result = await upsertDailyDownloads(qx, id, [
        { day: '2026-11-10', downloads: 10 },
        { day: '2026-11-11', downloads: 12 },
      ])

      expect(result).toEqual({ inserted: 2, overwritten: 0 })
      expect(await storedCount(id, '2026-11-11')).toBe(12)
    })

    it('overwrites a stored zero with the count npm published later (regression)', async () => {
      const { id } = await makePackage('overwrite-zero')
      await upsertDailyDownloads(qx, id, [{ day: '2026-11-09', downloads: 0 }])

      const result = await upsertDailyDownloads(qx, id, [
        { day: '2026-11-09', downloads: 120 },
        { day: '2026-11-10', downloads: 7 },
      ])

      expect(result).toEqual({ inserted: 1, overwritten: 1 })
      expect(await storedCount(id, '2026-11-09')).toBe(120)
    })

    it('leaves a matching count untouched', async () => {
      const { id } = await makePackage('same-count')
      await upsertDailyDownloads(qx, id, [{ day: '2026-11-09', downloads: 50 }])
      const before = await qx.selectOne(
        `SELECT updated_at FROM downloads_daily WHERE package_id = $(id)::bigint AND date = '2026-11-09'`,
        { id },
      )

      const result = await upsertDailyDownloads(qx, id, [{ day: '2026-11-09', downloads: 50 }])

      expect(result).toEqual({ inserted: 0, overwritten: 0 })
      const after = await qx.selectOne(
        `SELECT updated_at FROM downloads_daily WHERE package_id = $(id)::bigint AND date = '2026-11-09'`,
        { id },
      )
      expect(after.updated_at).toEqual(before.updated_at)
    })

    it('keeps a stored count when npm answers zero for that day', async () => {
      const { id } = await makePackage('zero-never-wins')
      await upsertDailyDownloads(qx, id, [{ day: '2026-11-09', downloads: 50 }])

      const result = await upsertDailyDownloads(qx, id, [{ day: '2026-11-09', downloads: 0 }])

      expect(result).toEqual({ inserted: 0, overwritten: 0 })
      expect(await storedCount(id, '2026-11-09')).toBe(50)
    })
  })

  describe('getNewestDownloadDate', () => {
    it('returns the newest stored day up to the bound', async () => {
      const { id } = await makePackage('newest-day')
      await upsertDailyDownloads(qx, id, [
        { day: '2026-11-01', downloads: 1 },
        { day: '2026-11-05', downloads: 1 },
        { day: '2026-11-12', downloads: 1 },
      ])

      expect(await getNewestDownloadDate(qx, id, '2026-11-11')).toBe('2026-11-05')
    })

    it('returns null when nothing is stored', async () => {
      const { id } = await makePackage('nothing-stored')

      expect(await getNewestDownloadDate(qx, id, '2026-11-11')).toBeNull()
    })
  })

  describe('getNpmPackagesNeedingDailyBackfill', () => {
    async function makeSelectionFixtures(): Promise<Record<string, string>> {
      const fixtures = {
        unscopedRecent: await makePackage('unscoped-recent', {
          processedAt: '2001-01-10T02:30:00Z',
        }),
        scoped3d: await makePackage('scoped-3d', {
          scope: FIXTURE_TAG,
          processedAt: '2001-01-07T03:30:00Z',
        }),
        scoped7d: await makePackage('scoped-7d', {
          scope: FIXTURE_TAG,
          processedAt: '2001-01-03T03:30:00Z',
        }),
        scopedNever: await makePackage('scoped-never', { scope: FIXTURE_TAG }),
        scopedNotCritical: await makePackage('scoped-not-critical', {
          scope: FIXTURE_TAG,
          critical: false,
        }),
      }
      return Object.fromEntries(Object.entries(fixtures).map(([k, v]) => [v.purl, k]))
    }

    async function selected(scopedMinAgeDays: number, byPurl: Record<string, string>) {
      const rows = await getNpmPackagesNeedingDailyBackfill(
        qx,
        CUTOFF,
        100_000,
        0,
        1,
        scopedMinAgeDays,
      )
      return rows
        .map((r) => byPurl[r.purl])
        .filter(Boolean)
        .sort()
    }

    it('selects scoped packages only once they were processed 6 or more days before the cutoff', async () => {
      const byPurl = await makeSelectionFixtures()

      expect(await selected(6, byPurl)).toEqual(['scoped7d', 'scopedNever', 'unscopedRecent'])
    })

    it('selects every critical package processed before the cutoff when the interval is 0', async () => {
      const byPurl = await makeSelectionFixtures()

      expect(await selected(0, byPurl)).toEqual([
        'scoped3d',
        'scoped7d',
        'scopedNever',
        'unscopedRecent',
      ])
    })

    it('returns the npm registry name for scoped packages', async () => {
      const byPurl = await makeSelectionFixtures()
      const rows = await getNpmPackagesNeedingDailyBackfill(qx, CUTOFF, 100_000, 0, 1, 0)

      const scoped = rows.find((r) => byPurl[r.purl] === 'scopedNever')
      expect(scoped?.name).toBe(`@${FIXTURE_TAG}/scoped-never`)
    })
  })
})
