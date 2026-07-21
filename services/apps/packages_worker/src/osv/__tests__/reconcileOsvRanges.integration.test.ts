import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import {
  reconcileOsvRanges,
  supersedeDepsDevRanges,
} from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getDbConnection } from '@crowd/database'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing, matching deriveCriticalFlag.integration.test.ts.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_OSV_ID = 'osv-test-fixture-reconcile'

interface RangeRow {
  introduced_version: string | null
  fixed_version: string | null
  last_affected: string | null
  range_raw: string | null
  unaffected_raw: string | null
  deleted_at: Date | null
  updated_at: Date
}

async function cleanupFixture(qx: QueryExecutor): Promise<void> {
  await qx.result(
    `
    DELETE FROM advisory_affected_ranges
    WHERE advisory_package_id IN (
      SELECT ap.id FROM advisory_packages ap
      JOIN advisories a ON a.id = ap.advisory_id
      WHERE a.osv_id = $(osvId)
    )
    `,
    { osvId: FIXTURE_OSV_ID },
  )
  await qx.result(
    `DELETE FROM advisory_packages WHERE advisory_id IN (SELECT id FROM advisories WHERE osv_id = $(osvId))`,
    { osvId: FIXTURE_OSV_ID },
  )
  await qx.result(`DELETE FROM advisories WHERE osv_id = $(osvId)`, { osvId: FIXTURE_OSV_ID })
}

async function liveRanges(qx: QueryExecutor, advisoryPackageId: number): Promise<RangeRow[]> {
  return qx.select(
    `
    SELECT introduced_version, fixed_version, last_affected, range_raw, unaffected_raw, deleted_at, updated_at
    FROM advisory_affected_ranges
    WHERE advisory_package_id = $(advisoryPackageId)
    ORDER BY id
    `,
    { advisoryPackageId },
  )
}

describe.skipIf(!HAVE_DB)('reconcileOsvRanges / supersedeDepsDevRanges — real packages-db', () => {
  let qx: QueryExecutor
  let advisoryPackageId: number

  beforeAll(async () => {
    const conn = await getDbConnection({
      host: process.env.CROWD_PACKAGES_DB_WRITE_HOST ?? '',
      port: parseInt(process.env.CROWD_PACKAGES_DB_PORT ?? '0', 10),
      database: process.env.CROWD_PACKAGES_DB_DATABASE ?? '',
      user: process.env.CROWD_PACKAGES_DB_USERNAME ?? '',
      password: process.env.CROWD_PACKAGES_DB_PASSWORD ?? '',
    })
    qx = pgpQx(conn)
    await cleanupFixture(qx)

    const advisory = await qx.selectOne(
      `
      INSERT INTO advisories (osv_id, source, source_url, aliases, severity, cvss, cvss_source, created_at, updated_at)
      VALUES ($(osvId), 'GHSA', NULL, ARRAY[]::text[], 'HIGH', 7.5, 'osv_cvss_v3', NOW(), NOW())
      RETURNING id
      `,
      { osvId: FIXTURE_OSV_ID },
    )
    const advisoryPackage = await qx.selectOne(
      `
      INSERT INTO advisory_packages (advisory_id, package_id, ecosystem, package_name, created_at, updated_at)
      VALUES ($(advisoryId), NULL, 'npm', 'reconcile-fixture-pkg', NOW(), NOW())
      RETURNING id
      `,
      { advisoryId: advisory.id },
    )
    advisoryPackageId = advisoryPackage.id as number
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixture(qx)
  })

  it('inserts the initial OSV range set as live rows', async () => {
    await reconcileOsvRanges(qx, advisoryPackageId, [
      {
        advisoryPackageId,
        introducedVersion: '1.0.0',
        fixedVersion: '1.2.0',
        lastAffected: null,
      },
      {
        advisoryPackageId,
        introducedVersion: '2.0.0',
        fixedVersion: null,
        lastAffected: '2.5.0',
      },
    ])

    const rows = await liveRanges(qx, advisoryPackageId)
    expect(rows).toHaveLength(2)
    expect(rows.every((r) => r.deleted_at === null)).toBe(true)
  })

  it('leaves an unchanged tuple untouched on a no-op resync (no updated_at bump)', async () => {
    const before = await liveRanges(qx, advisoryPackageId)
    const beforeUpdatedAt = before.find((r) => r.introduced_version === '1.0.0')?.updated_at

    await new Promise((resolve) => setTimeout(resolve, 1100))
    await reconcileOsvRanges(qx, advisoryPackageId, [
      {
        advisoryPackageId,
        introducedVersion: '1.0.0',
        fixedVersion: '1.2.0',
        lastAffected: null,
      },
      {
        advisoryPackageId,
        introducedVersion: '2.0.0',
        fixedVersion: null,
        lastAffected: '2.5.0',
      },
    ])

    const after = await liveRanges(qx, advisoryPackageId)
    const afterUpdatedAt = after.find((r) => r.introduced_version === '1.0.0')?.updated_at
    expect(afterUpdatedAt?.getTime()).toBe(beforeUpdatedAt?.getTime())
  })

  it('soft-deletes a stale tuple dropped from the new range set', async () => {
    await reconcileOsvRanges(qx, advisoryPackageId, [
      {
        advisoryPackageId,
        introducedVersion: '1.0.0',
        fixedVersion: '1.2.0',
        lastAffected: null,
      },
      // '2.0.0'..'2.5.0' range dropped — OSV no longer reports it.
    ])

    const rows = await qx.select(
      `
      SELECT introduced_version, deleted_at
      FROM advisory_affected_ranges
      WHERE advisory_package_id = $(advisoryPackageId) AND introduced_version = '2.0.0'
      `,
      { advisoryPackageId },
    )
    expect(rows).toHaveLength(1)
    expect(rows[0].deleted_at).not.toBeNull()
  })

  it('revives a tombstoned tuple that reappears in a later sync', async () => {
    await reconcileOsvRanges(qx, advisoryPackageId, [
      {
        advisoryPackageId,
        introducedVersion: '1.0.0',
        fixedVersion: '1.2.0',
        lastAffected: null,
      },
      {
        advisoryPackageId,
        introducedVersion: '2.0.0',
        fixedVersion: null,
        lastAffected: '2.5.0',
      },
    ])

    const rows = await qx.select(
      `
      SELECT deleted_at
      FROM advisory_affected_ranges
      WHERE advisory_package_id = $(advisoryPackageId) AND introduced_version = '2.0.0'
      `,
      { advisoryPackageId },
    )
    expect(rows).toHaveLength(1)
    expect(rows[0].deleted_at).toBeNull()
  })

  it('supersedes a live deps.dev raw row once OSV owns the package', async () => {
    await qx.result(
      `
      INSERT INTO advisory_affected_ranges
        (advisory_package_id, range_raw, unaffected_raw, introduced_version, created_at, updated_at)
      VALUES ($(advisoryPackageId), '>=1.0.0 <1.2.0', NULL, NULL, NOW(), NOW())
      `,
      { advisoryPackageId },
    )

    await supersedeDepsDevRanges(qx, advisoryPackageId)

    const rows = await qx.select(
      `
      SELECT deleted_at
      FROM advisory_affected_ranges
      WHERE advisory_package_id = $(advisoryPackageId) AND range_raw = '>=1.0.0 <1.2.0'
      `,
      { advisoryPackageId },
    )
    expect(rows).toHaveLength(1)
    expect(rows[0].deleted_at).not.toBeNull()
  })
})
