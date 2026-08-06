import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import { getReverseDependents, getVersionNumbers } from './blastRadiusDependents'

// Integration test: hits the running packages-db. Skipped automatically when any of
// the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'akrites-blast-radius-dependents-fixture'

describe.skipIf(!HAVE_DB)('getReverseDependents — real packages-db', () => {
  let qx: QueryExecutor
  let dependsOnId: string

  async function cleanupFixtures(): Promise<void> {
    await qx.result(
      `DELETE FROM package_dependencies WHERE depends_on_id IN (
         SELECT id FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(
      `DELETE FROM versions WHERE package_id IN (
         SELECT id FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
  }

  async function makePackage(
    name: string,
    counts: { dependentCount?: number; dependentReposCount?: number } = {},
  ): Promise<string> {
    const purl = `pkg:golang/${name}`
    const row = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, namespace, name, status, ingestion_source,
                             dependent_count, dependent_repos_count)
       VALUES ($(purl), 'go', NULL, $(name), 'active', $(tag), $(dependentCount), $(dependentReposCount))
       RETURNING id`,
      {
        purl,
        name,
        tag: FIXTURE_TAG,
        dependentCount: counts.dependentCount ?? null,
        dependentReposCount: counts.dependentReposCount ?? null,
      },
    )
    return row.id
  }

  async function makeVersion(
    packageId: string,
    number: string,
    opts: { isLatest?: boolean } = {},
  ): Promise<string> {
    const row = await qx.selectOne(
      `INSERT INTO versions (package_id, ecosystem, number, name, is_latest)
       VALUES ($(packageId), 'go', $(number), $(number), $(isLatest))
       RETURNING id`,
      { packageId, number, isLatest: opts.isLatest ?? null },
    )
    return row.id
  }

  async function makeDependency(
    packageId: string,
    versionId: string,
    ownDependsOnId: string,
    versionConstraint: string,
  ): Promise<void> {
    await qx.result(
      `INSERT INTO package_dependencies
         (package_id, version_id, depends_on_id, version_constraint, dependency_kind)
       VALUES ($(packageId), $(versionId), $(dependsOnId), $(versionConstraint), 'direct')`,
      { packageId, versionId, dependsOnId: ownDependsOnId, versionConstraint },
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

    dependsOnId = await makePackage('github.com/pubnub/go/v7')

    const depA = await makePackage('github.com/a/high-impact', {
      dependentCount: 5,
      dependentReposCount: 50,
    })
    const depAVersion = await makeVersion(depA, 'v1.0.0')
    await makeDependency(depA, depAVersion, dependsOnId, 'v1.0.0')

    const depB = await makePackage('github.com/b/low-impact', {
      dependentCount: 1,
      dependentReposCount: 1,
    })
    const depBVersion = await makeVersion(depB, 'v1.0.0')
    await makeDependency(depB, depBVersion, dependsOnId, 'v0.5.0')

    // Different ecosystem, same depends_on_id target row shape — must never surface.
    const otherEcosystemPkg = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, namespace, name, status, ingestion_source)
       VALUES ($(purl), 'npm', NULL, $(name), 'active', $(tag)) RETURNING id`,
      {
        purl: `pkg:npm/${FIXTURE_TAG}-other-ecosystem`,
        name: `${FIXTURE_TAG}-other-ecosystem`,
        tag: FIXTURE_TAG,
      },
    )
    const otherEcosystemVersion = await makeVersion(otherEcosystemPkg.id, '1.0.0')
    await makeDependency(otherEcosystemPkg.id, otherEcosystemVersion, dependsOnId, '^1.0.0')

    await makeVersion(dependsOnId, 'v7.0.0')
    await makeVersion(dependsOnId, 'v7.1.0')

    // Same dependent package, two historical versions both requiring dependsOnId —
    // getReverseDependents must collapse these to a single row (the is_latest one),
    // not let both consume the LIMIT.
    const depC = await makePackage('github.com/c/multi-version', {
      dependentCount: 3,
      dependentReposCount: 10,
    })
    const depCOldVersion = await makeVersion(depC, 'v1.0.0', { isLatest: false })
    await makeDependency(depC, depCOldVersion, dependsOnId, '>=v1.0.0')
    const depCLatestVersion = await makeVersion(depC, 'v2.0.0', { isLatest: true })
    await makeDependency(depC, depCLatestVersion, dependsOnId, '>=v2.0.0')
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures()
  })

  it('returns only same-ecosystem dependents, ranked by dependent_repos_count desc', async () => {
    const rows = await getReverseDependents(qx, dependsOnId, 'go', 10)

    expect(rows.map((r) => r.name)).toEqual([
      'github.com/a/high-impact',
      'github.com/c/multi-version',
      'github.com/b/low-impact',
    ])
    expect(rows[0].versionConstraint).toBe('v1.0.0')
    expect(rows[0].versionNumber).toBe('v1.0.0')
    expect(rows[0].dependencyKind).toBe('direct')
  })

  it('respects the limit parameter', async () => {
    const rows = await getReverseDependents(qx, dependsOnId, 'go', 1)
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('github.com/a/high-impact')
  })

  it('collapses a dependent with multiple historical versions to its is_latest one', async () => {
    const rows = await getReverseDependents(qx, dependsOnId, 'go', 10)
    const multiVersionRows = rows.filter((r) => r.name === 'github.com/c/multi-version')

    expect(multiVersionRows).toHaveLength(1)
    expect(multiVersionRows[0].versionNumber).toBe('v2.0.0')
    expect(multiVersionRows[0].versionConstraint).toBe('>=v2.0.0')
  })

  it('returns an empty list for an ecosystem with no matching dependents', async () => {
    const rows = await getReverseDependents(qx, dependsOnId, 'maven', 10)
    expect(rows).toEqual([])
  })

  it('getVersionNumbers returns the ingested version numbers for a package', async () => {
    const numbers = await getVersionNumbers(qx, dependsOnId)
    expect(numbers.sort()).toEqual(['v7.0.0', 'v7.1.0'])
  })
})
