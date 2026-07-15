import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import { getPackageDetailsByPurls } from './api'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'akrites-external-package-detail-fixture'

describe.skipIf(!HAVE_DB)('getPackageDetailsByPurls — real packages-db', () => {
  let qx: QueryExecutor
  let linkedPackageId: number

  const linkedPurl = `pkg:test-fixture/${FIXTURE_TAG}/linked`
  const unlinkedPurl = `pkg:test-fixture/${FIXTURE_TAG}/unlinked`
  const missingPurl = `pkg:test-fixture/${FIXTURE_TAG}/does-not-exist`
  const repoUrl = `https://github.com/example/${FIXTURE_TAG}-repo`

  async function cleanupFixtures(): Promise<void> {
    await qx.result(
      `DELETE FROM package_repos WHERE package_id IN (SELECT id FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(
      `DELETE FROM package_maintainers WHERE package_id IN (SELECT id FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
    await qx.result(`DELETE FROM maintainers WHERE ecosystem = $(tag)`, { tag: FIXTURE_TAG })
    await qx.result(`DELETE FROM repos WHERE url = $(url)`, { url: repoUrl })
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

    const linkedRow = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, name, declared_repository_url, repository_url, ingestion_source, status)
       VALUES ($(purl), 'npm', 'linked-fixture', $(declared), $(canonical), $(tag), 'active')
       RETURNING id`,
      {
        purl: linkedPurl,
        declared: 'https://github.com/example/linked-fixture',
        canonical: 'https://github.com/example/linked-fixture',
        tag: FIXTURE_TAG,
      },
    )
    linkedPackageId = linkedRow.id as number

    await qx.result(
      `INSERT INTO packages (purl, ecosystem, name, declared_repository_url, repository_url, ingestion_source, status)
       VALUES ($(purl), 'npm', 'unlinked-fixture', $(declared), $(canonical), $(tag), 'active')`,
      {
        purl: unlinkedPurl,
        declared: 'https://github.com/example/unlinked-fixture-raw',
        canonical: 'https://github.com/example/unlinked-fixture-canonical',
        tag: FIXTURE_TAG,
      },
    )

    const repoRow = await qx.selectOne(`INSERT INTO repos (url) VALUES ($(url)) RETURNING id`, {
      url: repoUrl,
    })

    await qx.result(
      `INSERT INTO package_repos (package_id, repo_id, source, confidence)
       VALUES ($(packageId), $(repoId), 'declared', 0.90)`,
      { packageId: linkedPackageId, repoId: repoRow.id },
    )

    for (const username of ['maintainer-one', 'maintainer-two']) {
      const maintainer = await qx.selectOne(
        `INSERT INTO maintainers (ecosystem, username) VALUES ($(ecosystem), $(username)) RETURNING id`,
        { ecosystem: FIXTURE_TAG, username },
      )
      await qx.result(
        `INSERT INTO package_maintainers (package_id, maintainer_id, role)
         VALUES ($(packageId), $(maintainerId), 'maintainer')`,
        { packageId: linkedPackageId, maintainerId: maintainer.id },
      )
    }
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures()
  })

  it('omits purls with no matching package', async () => {
    const rows = await getPackageDetailsByPurls(qx, [missingPurl])
    expect(rows).toHaveLength(0)
  })

  it('returns one row per matched purl in a batch, ignoring the ones that miss', async () => {
    const rows = await getPackageDetailsByPurls(qx, [missingPurl, unlinkedPurl, linkedPurl])
    expect(rows.map((r) => r.purl).sort()).toEqual([linkedPurl, unlinkedPurl].sort())
  })

  it('resolves resolvedRepositoryUrl via the package_repos/repos join when linked', async () => {
    const [row] = await getPackageDetailsByPurls(qx, [linkedPurl])
    expect(row.resolvedRepositoryUrl).toBe(repoUrl)
    expect(Number(row.repoMappingConfidence)).toBeCloseTo(0.9)
  })

  it('leaves resolvedRepositoryUrl null but still exposes the raw repositoryUrl column when unlinked', async () => {
    const [row] = await getPackageDetailsByPurls(qx, [unlinkedPurl])
    expect(row.resolvedRepositoryUrl).toBeNull()
    expect(row.repositoryUrl).toBe('https://github.com/example/unlinked-fixture-canonical')
    expect(row.declaredRepositoryUrl).toBe('https://github.com/example/unlinked-fixture-raw')
  })

  it('aggregates maintainerCount from package_maintainers', async () => {
    const rows = await getPackageDetailsByPurls(qx, [linkedPurl, unlinkedPurl])
    const byPurl = new Map(rows.map((r) => [r.purl, r]))
    expect(byPurl.get(linkedPurl)?.maintainerCount).toBe(2)
    expect(byPurl.get(unlinkedPurl)?.maintainerCount).toBe(0)
  })
})
