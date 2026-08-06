import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import { getAdvisoriesByPurls } from './api'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'akrites-external-advisory-fixture'
const OSV_OPEN = `${FIXTURE_TAG}-GHSA-critical-open`
const OSV_PATCHED = `${FIXTURE_TAG}-GHSA-high-patched`

describe.skipIf(!HAVE_DB)('getAdvisoriesByPurls — real packages-db', () => {
  let qx: QueryExecutor

  const withAdvisoriesPurl = `pkg:test-fixture/${FIXTURE_TAG}/with-advisories`
  const noAdvisoriesPurl = `pkg:test-fixture/${FIXTURE_TAG}/no-advisories`
  const missingPurl = `pkg:test-fixture/${FIXTURE_TAG}/does-not-exist`

  async function cleanupFixtures(): Promise<void> {
    await qx.result(
      `DELETE FROM advisory_affected_ranges WHERE advisory_package_id IN (
         SELECT ap.id FROM advisory_packages ap
         JOIN packages p ON p.id = ap.package_id
         WHERE p.ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(
      `DELETE FROM advisory_packages WHERE package_id IN (SELECT id FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(`DELETE FROM advisories WHERE osv_id LIKE $(prefix)`, {
      prefix: `${FIXTURE_TAG}-%`,
    })
    await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
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

    // Package with two advisories (latest_version 2.0.0 drives resolution below).
    const withRow = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, name, latest_version, ingestion_source, status)
       VALUES ($(purl), 'npm', 'with-advisories-fixture', '2.0.0', $(tag), 'active')
       RETURNING id`,
      { purl: withAdvisoriesPurl, tag: FIXTURE_TAG },
    )
    const withPackageId = withRow.id as string

    // Package that exists but has no advisories at all.
    await qx.result(
      `INSERT INTO packages (purl, ecosystem, name, latest_version, ingestion_source, status)
       VALUES ($(purl), 'npm', 'no-advisories-fixture', '1.0.0', $(tag), 'active')`,
      { purl: noAdvisoriesPurl, tag: FIXTURE_TAG },
    )

    // Critical advisory, fixed at 3.0.0 → latest 2.0.0 is still affected → 'open'.
    const openAdvisory = await qx.selectOne(
      `INSERT INTO advisories (osv_id, severity, cvss) VALUES ($(osv), 'CRITICAL', 9.1) RETURNING id`,
      { osv: OSV_OPEN },
    )
    // High advisory, fixed at 1.5.0 → latest 2.0.0 has the fix → 'patched'.
    const patchedAdvisory = await qx.selectOne(
      `INSERT INTO advisories (osv_id, severity, cvss) VALUES ($(osv), 'HIGH', 7.5) RETURNING id`,
      { osv: OSV_PATCHED },
    )

    for (const [advisoryId, fixedVersion] of [
      [openAdvisory.id, '3.0.0'],
      [patchedAdvisory.id, '1.5.0'],
    ] as const) {
      const ap = await qx.selectOne(
        `INSERT INTO advisory_packages (advisory_id, package_id, ecosystem, package_name)
         VALUES ($(advisoryId), $(packageId), 'npm', 'with-advisories-fixture') RETURNING id`,
        { advisoryId, packageId: withPackageId },
      )
      await qx.result(
        `INSERT INTO advisory_affected_ranges (advisory_package_id, introduced_version, fixed_version)
         VALUES ($(apId), '1.0.0', $(fixed))`,
        { apId: ap.id, fixed: fixedVersion },
      )
    }
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures()
  })

  it('returns no rows for a purl that does not resolve to a package', async () => {
    const rows = await getAdvisoriesByPurls(qx, [missingPurl])
    expect(rows).toHaveLength(0)
  })

  it('returns a single null-osvId sentinel row for a found package with no advisories', async () => {
    const rows = await getAdvisoriesByPurls(qx, [noAdvisoriesPurl])
    expect(rows).toHaveLength(1)
    expect(rows[0].purl).toBe(noAdvisoriesPurl)
    expect(rows[0].osvId).toBeNull()
  })

  it('returns one row per advisory with resolution derived from the latest version', async () => {
    const rows = await getAdvisoriesByPurls(qx, [withAdvisoriesPurl])
    const byOsv = new Map(rows.map((r) => [r.osvId, r]))
    expect(rows).toHaveLength(2)
    expect(byOsv.get(OSV_OPEN)?.resolution).toBe('open')
    expect(byOsv.get(OSV_OPEN)?.severity).toBe('critical')
    expect(byOsv.get(OSV_OPEN)?.isCritical).toBe(true)
    expect(byOsv.get(OSV_PATCHED)?.resolution).toBe('patched')
    expect(byOsv.get(OSV_PATCHED)?.severity).toBe('high')
  })

  it('batches many purls, omitting the misses and keeping found-but-empty packages', async () => {
    const rows = await getAdvisoriesByPurls(qx, [missingPurl, noAdvisoriesPurl, withAdvisoriesPurl])
    const purls = new Set(rows.map((r) => r.purl))
    expect(purls.has(missingPurl)).toBe(false)
    expect(purls.has(noAdvisoriesPurl)).toBe(true)
    expect(purls.has(withAdvisoriesPurl)).toBe(true)
  })
})
