import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getDbConnection } from '@crowd/database'

import { backfillAllPackageRepoOwnershipMatch } from '../backfillOwnershipMatch'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing, matching deriveCriticalFlag.integration.test.ts.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'ownership-match-backfill-fixture'

interface PackageFixture {
  ecosystem: string
  namespace: string | null
  name: string
  maintainers?: string[]
}

interface RepoFixture {
  repoOwner: string
  source: string
  host?: string
  repoUrlOverride?: string
}

// declared package_repos rows, all left on the migration default 'no_evidence',
// covering: namespace match via reverse-DNS segmentation, maintainer-only match,
// no evidence lining up, a non-'declared' source that the backfill must skip, a
// host='other' repo whose owner column is a URL-parsing artifact (e.g. 'repos'
// from an apache svn/gitbox path) rather than a real owner, and Go's divergent
// module-path-based ownership evidence (package_repo_go_repo_owner /
// package_repo_go_module_owner).
const FIXTURES: Record<string, PackageFixture & RepoFixture> = {
  namespaceMatch: {
    ecosystem: 'maven',
    namespace: 'io.github.lehadnk',
    name: 'some-lib',
    repoOwner: 'lehadnk',
    source: 'declared',
  },
  namespaceNoMatch: {
    ecosystem: 'maven',
    namespace: 'org.apache.tamaya.ext.examples',
    name: 'tamaya-examples',
    repoOwner: 'unrelatedvendor',
    source: 'declared',
  },
  maintainerMatch: {
    ecosystem: 'npm',
    namespace: null,
    name: 'acme-tool',
    maintainers: ['seldaek'],
    repoOwner: `${FIXTURE_TAG}-seldaek`,
    source: 'declared',
  },
  nonDeclaredSkipped: {
    ecosystem: 'npm',
    namespace: '@vercel',
    name: 'next-clone',
    repoOwner: 'vercel',
    source: 'deps_dev',
  },
  otherHostOwnerArtifact: {
    ecosystem: 'maven',
    namespace: 'org.apache.commons',
    name: 'commons-cli',
    repoOwner: 'repos',
    source: 'declared',
    host: 'other',
  },
  goOtherHostVcsMatch: {
    ecosystem: 'go',
    namespace: null,
    name: `codeberg.org/${FIXTURE_TAG}-goowner/gomodule`,
    repoOwner: `${FIXTURE_TAG}-goowner`,
    source: 'declared',
    host: 'other',
    repoUrlOverride: `https://codeberg.org/${FIXTURE_TAG}-goowner/gomodule`,
  },
  goStandardHostMatch: {
    ecosystem: 'go',
    namespace: null,
    name: `github.com/${FIXTURE_TAG}-ghowner/gomodule2`,
    repoOwner: `${FIXTURE_TAG}-ghowner`,
    source: 'declared',
  },
  goVanityModuleNoMatch: {
    ecosystem: 'go',
    namespace: null,
    name: 'k8s.io/client-go',
    repoOwner: `${FIXTURE_TAG}-vanityowner`,
    source: 'declared',
  },
}

async function cleanupFixtures(qx: QueryExecutor): Promise<void> {
  await qx.result(
    `DELETE FROM package_maintainers WHERE package_id IN (SELECT id FROM packages WHERE purl LIKE $(prefix))`,
    { prefix: `pkg:${FIXTURE_TAG}/%` },
  )
  await qx.result(`DELETE FROM maintainers WHERE username LIKE $(prefix)`, {
    prefix: `${FIXTURE_TAG}-%`,
  })
  await qx.result(
    `DELETE FROM package_repos WHERE package_id IN (SELECT id FROM packages WHERE purl LIKE $(prefix))`,
    { prefix: `pkg:${FIXTURE_TAG}/%` },
  )
  await qx.result(`DELETE FROM packages WHERE purl LIKE $(prefix)`, {
    prefix: `pkg:${FIXTURE_TAG}/%`,
  })
  await qx.result(`DELETE FROM repos WHERE url LIKE $(prefix)`, {
    prefix: `%${FIXTURE_TAG}%`,
  })
}

async function insertFixture(
  qx: QueryExecutor,
  key: string,
  f: PackageFixture & RepoFixture,
): Promise<number> {
  const purl = `pkg:${FIXTURE_TAG}/${key}`
  const pkg = await qx.selectOne(
    `
    INSERT INTO packages (purl, ecosystem, namespace, name, status)
    VALUES ($(purl), $(ecosystem), $(namespace), $(name), 'active')
    RETURNING id
    `,
    { purl, ecosystem: f.ecosystem, namespace: f.namespace, name: f.name },
  )

  const host = f.host ?? 'github'
  // The backfill parses owner from the URL path (package_repo_owner_from_url), not the
  // stored owner column, so repoOwner must appear in the URL for host != 'other'.
  const repoUrl =
    f.repoUrlOverride ??
    (host === 'github'
      ? `https://github.com/${f.repoOwner}/${FIXTURE_TAG}-${key}`
      : `https://svn.apache.org/${FIXTURE_TAG}/${key}`)
  const repo = await qx.selectOne(
    `
    INSERT INTO repos (url, host, owner, name)
    VALUES ($(url), $(host), $(owner), $(key))
    RETURNING id
    `,
    { url: repoUrl, host, owner: f.repoOwner, key },
  )

  if (f.maintainers?.length) {
    for (const username of f.maintainers) {
      const maintainer = await qx.selectOne(
        `
        INSERT INTO maintainers (ecosystem, username)
        VALUES ($(ecosystem), $(username))
        ON CONFLICT (ecosystem, username) DO UPDATE SET username = EXCLUDED.username
        RETURNING id
        `,
        { ecosystem: f.ecosystem, username: `${FIXTURE_TAG}-${username}` },
      )
      await qx.result(
        `
        INSERT INTO package_maintainers (package_id, maintainer_id, role)
        VALUES ($(packageId), $(maintainerId), 'maintainer')
        `,
        { packageId: pkg.id, maintainerId: maintainer.id },
      )
    }
  }

  await qx.result(
    `
    INSERT INTO package_repos (package_id, repo_id, source, confidence)
    VALUES ($(packageId), $(repoId), $(source), 0.50)
    `,
    { packageId: pkg.id, repoId: repo.id, source: f.source },
  )

  return pkg.id as number
}

async function ownershipMatchFor(qx: QueryExecutor, packageId: number): Promise<string> {
  const row = await qx.selectOne(
    `SELECT ownership_match FROM package_repos WHERE package_id = $(packageId)`,
    { packageId },
  )
  return row.ownership_match as string
}

describe.skipIf(!HAVE_DB)('backfillAllPackageRepoOwnershipMatch — real packages-db', () => {
  let qx: QueryExecutor
  const ids: Record<string, number> = {}

  beforeAll(async () => {
    const conn = await getDbConnection({
      host: process.env.CROWD_PACKAGES_DB_WRITE_HOST ?? '',
      port: parseInt(process.env.CROWD_PACKAGES_DB_PORT ?? '0', 10),
      database: process.env.CROWD_PACKAGES_DB_DATABASE ?? '',
      user: process.env.CROWD_PACKAGES_DB_USERNAME ?? '',
      password: process.env.CROWD_PACKAGES_DB_PASSWORD ?? '',
    })
    qx = pgpQx(conn)
    await cleanupFixtures(qx)
    for (const [key, f] of Object.entries(FIXTURES)) {
      ids[key] = await insertFixture(qx, key, f)
    }
    await backfillAllPackageRepoOwnershipMatch(1000)
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures(qx)
  })

  it('matches via reverse-DNS namespace segmentation (io.github.lehadnk -> lehadnk)', async () => {
    expect(await ownershipMatchFor(qx, ids.namespaceMatch)).toBe('matched')
  })

  it('reports unmatched when namespace segments do not line up with the repo owner', async () => {
    expect(await ownershipMatchFor(qx, ids.namespaceNoMatch)).toBe('unmatched')
  })

  it('matches via maintainer username when the namespace gives no evidence', async () => {
    expect(await ownershipMatchFor(qx, ids.maintainerMatch)).toBe('matched')
  })

  it('leaves non-declared rows untouched at the default no_evidence', async () => {
    expect(await ownershipMatchFor(qx, ids.nonDeclaredSkipped)).toBe('no_evidence')
  })

  it('does not trust repos.owner for host=other rows (URL-parsing artifact, not a real owner)', async () => {
    expect(await ownershipMatchFor(qx, ids.otherHostOwnerArtifact)).toBe('no_evidence')
  })

  it('matches Go repos on host=other VCS forges (codeberg) via URL-path owner extraction', async () => {
    expect(await ownershipMatchFor(qx, ids.goOtherHostVcsMatch)).toBe('matched')
  })

  it('matches Go repos on a standard host via module-path owner extraction', async () => {
    expect(await ownershipMatchFor(qx, ids.goStandardHostMatch)).toBe('matched')
  })

  it('reports no evidence for a Go vanity import path not rooted at a known VCS host', async () => {
    expect(await ownershipMatchFor(qx, ids.goVanityModuleNoMatch)).toBe('no_evidence')
  })
})
