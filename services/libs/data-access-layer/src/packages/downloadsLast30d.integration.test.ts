import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import { insertLast30dDownloadIfAbsent } from './downloadsLast30d'

// Integration test: hits the running packages-db. Skipped automatically when any of
// the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'akrites-downloads-last-30d-fixture'

// insertLast30dDownloadIfAbsent is packagist's "first observation wins" guard for the
// monthly rolling window: the presence check, insert, and packages mirror all happen
// in one statement (ON CONFLICT DO NOTHING) so a genuine race between two callers for
// the same purl+month can never let a later write silently overwrite an earlier one.
describe.skipIf(!HAVE_DB)('insertLast30dDownloadIfAbsent — real packages-db', () => {
  let qx: QueryExecutor

  async function cleanupFixtures(): Promise<void> {
    await qx.result(
      `DELETE FROM downloads_last_30d WHERE purl IN (
         SELECT purl FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
  }

  async function makePackage(purl: string): Promise<void> {
    await qx.result(
      `INSERT INTO packages (purl, ecosystem, namespace, name, registry_url, status, ingestion_source)
       VALUES ($(purl), 'packagist', 'fixture', $(purl), 'https://example.test', 'active', $(tag))`,
      { purl, tag: FIXTURE_TAG },
    )
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
    await cleanupFixtures()
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures()
  })

  it('inserts the window and mirrors it to packages on the first call', async () => {
    const purl = `pkg:composer/${FIXTURE_TAG}/first-call`
    await makePackage(purl)

    const changed = await insertLast30dDownloadIfAbsent(
      qx,
      purl,
      '2026-06-01',
      '2026-07-01',
      100,
      true,
    )

    expect(changed).toEqual(
      expect.arrayContaining(['downloads_last_30d.start_date', 'downloads_last_30d.count']),
    )
    expect(changed).toContain('packages.downloads_last_30d')

    const row = await qx.selectOne(`SELECT count FROM downloads_last_30d WHERE purl = $(purl)`, {
      purl,
    })
    expect(Number(row.count)).toBe(100)
    const pkg = await qx.selectOne(`SELECT downloads_last_30d FROM packages WHERE purl = $(purl)`, {
      purl,
    })
    expect(Number(pkg.downloads_last_30d)).toBe(100)
  })

  it('does not overwrite an existing window — later call with a different count is a no-op', async () => {
    const purl = `pkg:composer/${FIXTURE_TAG}/second-call-loses`
    await makePackage(purl)

    const first = await insertLast30dDownloadIfAbsent(
      qx,
      purl,
      '2026-06-01',
      '2026-07-01',
      100,
      true,
    )
    expect(first.length).toBeGreaterThan(0)

    const second = await insertLast30dDownloadIfAbsent(
      qx,
      purl,
      '2026-06-01',
      '2026-07-01',
      999,
      true,
    )
    expect(second).toEqual([])

    const row = await qx.selectOne(`SELECT count FROM downloads_last_30d WHERE purl = $(purl)`, {
      purl,
    })
    expect(Number(row.count)).toBe(100)
    const pkg = await qx.selectOne(`SELECT downloads_last_30d FROM packages WHERE purl = $(purl)`, {
      purl,
    })
    expect(Number(pkg.downloads_last_30d)).toBe(100)
  })

  it('resolves a genuine race between concurrent callers to exactly one consistent winner', async () => {
    const purl = `pkg:composer/${FIXTURE_TAG}/concurrent-race`
    await makePackage(purl)

    const [a, b] = await Promise.all([
      insertLast30dDownloadIfAbsent(qx, purl, '2026-06-01', '2026-07-01', 111, true),
      insertLast30dDownloadIfAbsent(qx, purl, '2026-06-01', '2026-07-01', 222, true),
    ])

    // exactly one of the two concurrent callers observes itself as the writer
    const winners = [a, b].filter((c) => c.length > 0)
    expect(winners).toHaveLength(1)

    const row = await qx.selectOne(`SELECT count FROM downloads_last_30d WHERE purl = $(purl)`, {
      purl,
    })
    const pkg = await qx.selectOne(`SELECT downloads_last_30d FROM packages WHERE purl = $(purl)`, {
      purl,
    })
    // whichever value won, the window row and its packages mirror must agree
    expect(Number(pkg.downloads_last_30d)).toBe(Number(row.count))
    expect([111, 222]).toContain(Number(row.count))
  })
})
