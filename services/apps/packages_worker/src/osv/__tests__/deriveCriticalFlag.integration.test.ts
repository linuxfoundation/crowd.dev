import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getDbConnection } from '@crowd/database'

import { deriveCriticalFlag } from '../deriveCriticalFlag'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing so unit-test runs in CI stay green and
// a half-set env (host/port but no database) doesn't fail with a confusing
// connection error inside beforeAll.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'osv-test-fixture'

interface FixturePackage {
  ecosystem: string
  namespace: string | null
  name: string
  latest_version: string
}

// One row per real package — we UPDATE latest_version between assertions so the
// derive function sees both the "in-range" and "above-the-fix" cases without
// violating the (ecosystem, namespace, name) unique index. This also exercises
// the FALSE-clearing path: the second derive must un-flag a row the first
// derive flagged.
const FIXTURES: Record<string, FixturePackage> = {
  // lodash has multiple critical OSV advisories. The widest one (GHSA-r5fr-...)
  // covers [4.0.0, 4.18.0). We test the upper boundary by setting latest_version
  // above 4.18.0 in the cleared-case test — anything below is still vulnerable
  // to at least one of the five critical advisories on lodash.
  lodash: { ecosystem: 'npm', namespace: null, name: 'lodash', latest_version: '4.17.20' },
  // log4shell ranges include [2.13.0, 2.15.0). 2.14.1 in, 2.17.0 above the
  // related CVE-2021-45046/45105 chain too.
  log4j: {
    ecosystem: 'Maven',
    namespace: 'org.apache.logging.log4j',
    name: 'log4j-core',
    latest_version: '2.14.1',
  },
  // MAL- target — flag should flip via the osv_id LIKE 'MAL-%' branch
  // regardless of CVSS being NULL.
  cxpJquery: { ecosystem: 'npm', namespace: null, name: 'cxp-jquery', latest_version: '1.0.0' },
  // Regression guard for the implicit-empty-qualifier Maven bug. There's no
  // critical advisory for fixture.test:final-quirk, so the flag must be FALSE.
  finalQuirk: {
    ecosystem: 'Maven',
    namespace: 'fixture.test',
    name: 'final-quirk',
    latest_version: '1.0-final',
  },
}

async function cleanupFixtures(qx: QueryExecutor): Promise<void> {
  // advisory_packages may have populated package_id via the catch-up step on a
  // prior run; null them out before we delete the package rows.
  await qx.result(
    `UPDATE advisory_packages SET package_id = NULL
     WHERE package_id IN (SELECT id FROM packages WHERE ingestion_source = $(tag))`,
    { tag: FIXTURE_TAG },
  )
  await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
}

async function insertFixture(qx: QueryExecutor, p: FixturePackage, key: string): Promise<number> {
  const ns = p.namespace ?? ''
  const purl = `pkg:test-fixture/${key}/${p.ecosystem}/${ns}/${p.name}`
  const row = await qx.selectOne(
    `
    INSERT INTO packages
      (purl, ecosystem, namespace, name, latest_version, ingestion_source, status)
    VALUES
      ($(purl), $(ecosystem), $(namespace), $(name), $(latest_version), $(tag), 'active')
    RETURNING id
    `,
    { ...p, purl, tag: FIXTURE_TAG },
  )
  return row.id as number
}

async function setVersion(qx: QueryExecutor, id: number, version: string): Promise<void> {
  await qx.result(`UPDATE packages SET latest_version = $(version) WHERE id = $(id)`, {
    id,
    version,
  })
}

async function flag(qx: QueryExecutor, id: number): Promise<boolean> {
  const row = await qx.selectOne(
    `SELECT has_critical_vulnerability FROM packages WHERE id = $(id)`,
    { id },
  )
  return row.has_critical_vulnerability as boolean
}

describe.skipIf(!HAVE_DB)('deriveCriticalFlag — real packages-db', () => {
  let qx: QueryExecutor
  const ids: Record<string, number> = {}

  beforeAll(async () => {
    // HAVE_DB above already guarantees these env vars exist; fall through to
    // defensive defaults so the type is `string` without non-null assertions.
    const conn = await getDbConnection({
      host: process.env.CROWD_PACKAGES_DB_WRITE_HOST ?? '',
      port: parseInt(process.env.CROWD_PACKAGES_DB_PORT ?? '0', 10),
      database: process.env.CROWD_PACKAGES_DB_DATABASE ?? '',
      user: process.env.CROWD_PACKAGES_DB_USERNAME ?? '',
      password: process.env.CROWD_PACKAGES_DB_PASSWORD ?? '',
    })
    qx = pgpQx(conn)
    await cleanupFixtures(qx)
    for (const [key, p] of Object.entries(FIXTURES)) {
      ids[key] = await insertFixture(qx, p, key)
    }
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures(qx)
  })

  it('flips lodash 4.17.20 (inside the CVE range) to TRUE', async () => {
    await setVersion(qx, ids.lodash, '4.17.20')
    await deriveCriticalFlag(qx, 1000)
    expect(await flag(qx, ids.lodash)).toBe(true)
  })

  it('clears the lodash flag at 5.0.0 (above every critical fix)', async () => {
    // Five critical OSV advisories touch lodash, the widest fixed at 4.18.0.
    // Setting the package above every fix exercises the FALSE-clearing path:
    // the previous test left this row flagged TRUE; derive must un-flag it.
    await setVersion(qx, ids.lodash, '5.0.0')
    await deriveCriticalFlag(qx, 1000)
    expect(await flag(qx, ids.lodash)).toBe(false)
  })

  it('flips log4j-core 2.14.1 (inside log4shell range) to TRUE', async () => {
    await setVersion(qx, ids.log4j, '2.14.1')
    await deriveCriticalFlag(qx, 1000)
    expect(await flag(qx, ids.log4j)).toBe(true)
  })

  it('clears the log4j-core flag at 2.17.0 (above the patched range)', async () => {
    await setVersion(qx, ids.log4j, '2.17.0')
    await deriveCriticalFlag(qx, 1000)
    expect(await flag(qx, ids.log4j)).toBe(false)
  })

  it('flips a MAL- target via the osv_id LIKE prefix override', async () => {
    // cxp-jquery has cvss=NULL so this exercises the MAL- branch of the
    // derive predicate, not the cvss>=7.0 branch.
    await deriveCriticalFlag(qx, 1000)
    expect(await flag(qx, ids.cxpJquery)).toBe(true)
  })

  it('leaves a Maven 1.0-final package unflagged when no advisory matches', async () => {
    // Regression guard for the implicit-empty-qualifier bug in compareMaven —
    // if 1.0-final were treated as < 1.0, a range [0, 1.0) on any unrelated
    // advisory would spuriously flip the flag.
    expect(await flag(qx, ids.finalQuirk)).toBe(false)
  })

  it('catches up advisory_packages.package_id for late-arriving packages', async () => {
    // The MAL- advisories for cxp-jquery were ingested before the fixture
    // package existed; derive runs a catch-up UPDATE that resolves package_id.
    const row = await qx.selectOne(
      `
      SELECT COUNT(*) AS resolved
      FROM advisory_packages
      WHERE ecosystem = 'npm' AND package_name = 'cxp-jquery' AND package_id = $(id)
      `,
      { id: ids.cxpJquery },
    )
    expect(Number(row.resolved)).toBeGreaterThan(0)
  })
})
