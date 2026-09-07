import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import { BEST_REPO_LINK_JOIN } from '../osspckgs/sqlFragments'
import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import { upsertPackageRepo } from './repos'

// Integration test: hits the running packages-db, where V1788307200 defines
// package_repo_confidence and rescore_package_repo_confidence. Skipped when the DB env
// vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE = `cm1306-writes-${process.pid}`

type StoredLink = {
  source: string
  provenance: string | null
  confidence: number
}

describe.skipIf(!HAVE_DB)('package_repos write and rescore policy', () => {
  let qx: QueryExecutor
  let packageId: string
  let githubRepoId: string
  let gitlabRepoId: string

  async function storedLink(repoId: string): Promise<StoredLink> {
    return qx.selectOne(
      `SELECT source, provenance, confidence::float8 AS confidence
         FROM package_repos
        WHERE package_id = $(packageId)::bigint AND repo_id = $(repoId)::bigint`,
      { packageId, repoId },
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

    const pkg: { id: string } = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, name) VALUES ($(purl), 'npm', $(name))
       RETURNING id::text AS id`,
      { purl: `pkg:npm/${FIXTURE}@1.0.0`, name: FIXTURE },
    )
    packageId = pkg.id

    const github: { id: string } = await qx.selectOne(
      `INSERT INTO repos (url, host) VALUES ($(url), 'github') RETURNING id::text AS id`,
      { url: `https://github.com/${FIXTURE}/a` },
    )
    githubRepoId = github.id

    const gitlab: { id: string } = await qx.selectOne(
      `INSERT INTO repos (url, host) VALUES ($(url), 'gitlab') RETURNING id::text AS id`,
      { url: `https://gitlab.com/${FIXTURE}/b` },
    )
    gitlabRepoId = gitlab.id
  })

  afterAll(async () => {
    if (!packageId) return
    await qx.result(`DELETE FROM package_repos WHERE package_id = $(packageId)::bigint`, {
      packageId,
    })
    await qx.result(`DELETE FROM packages WHERE id = $(packageId)::bigint`, { packageId })
    await qx.result(`DELETE FROM repos WHERE id = ANY($(repoIds)::bigint[])`, {
      repoIds: [githubRepoId, gitlabRepoId],
    })
  })

  beforeEach(async () => {
    await qx.result(`DELETE FROM package_repos WHERE package_id = $(packageId)::bigint`, {
      packageId,
    })
    await qx.result(
      `UPDATE repos SET archived = NULL, is_fork = NULL, disabled = NULL
        WHERE id = ANY($(repoIds)::bigint[])`,
      { repoIds: [githubRepoId, gitlabRepoId] },
    )
  })

  describe('conflict policy', () => {
    it('keeps the stored row when a weaker source claims the same link', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'declared' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'heuristic' })

      const link = await storedLink(githubRepoId)
      expect(link.source).toBe('declared')
      expect(link.confidence).toBeCloseTo(0.85, 2)
    })

    it('hands the row to a stronger source', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'heuristic' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'manual' })

      const link = await storedLink(githubRepoId)
      expect(link.source).toBe('manual')
      expect(link.confidence).toBeCloseTo(0.99, 2)
    })

    it('is order-independent across sources', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'manual' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'declared' })
      const forward = await storedLink(githubRepoId)

      await qx.result(`DELETE FROM package_repos WHERE package_id = $(packageId)::bigint`, {
        packageId,
      })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'declared' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'manual' })
      const reverse = await storedLink(githubRepoId)

      expect(reverse.source).toBe(forward.source)
      expect(reverse.confidence).toBeCloseTo(forward.confidence, 9)
    })

    it('replaces the row when the same source restates a weaker claim', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, {
        source: 'deps_dev',
        provenance: 'SLSA_ATTESTATION',
      })
      expect((await storedLink(githubRepoId)).confidence).toBeCloseTo(0.99, 2)

      await upsertPackageRepo(qx, packageId, githubRepoId, {
        source: 'deps_dev',
        provenance: 'UNVERIFIED_METADATA',
      })

      const link = await storedLink(githubRepoId)
      expect(link.provenance).toBe('UNVERIFIED_METADATA')
      expect(link.confidence).toBeCloseTo(0.5, 2)
    })

    it('leaves the stored confidence consistent with the stored source', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'manual' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'heuristic' })

      const link = await storedLink(githubRepoId)
      const expected: { confidence: number } = await qx.selectOne(
        `SELECT package_repo_confidence(
           pr.source, p.ecosystem, pr.provenance,
           r.archived, r.is_fork, r.disabled, r.host, false, pr.repo_id
         )::float8 AS confidence
           FROM package_repos pr
           JOIN packages p ON p.id = pr.package_id
           JOIN repos r ON r.id = pr.repo_id
          WHERE pr.package_id = $(packageId)::bigint AND pr.repo_id = $(repoId)::bigint`,
        { packageId, repoId: githubRepoId },
      )
      expect(link.confidence).toBeCloseTo(expected.confidence, 9)
    })
  })

  describe('rescore_package_repo_confidence', () => {
    async function rescore(repoIds: string[]): Promise<void> {
      await qx.result(`CALL rescore_package_repo_confidence($(repoIds)::bigint[], 1000)`, {
        repoIds,
      })
    }

    it('applies repo state that arrived after the link was written', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'declared' })
      const before = await storedLink(githubRepoId)

      await qx.result(`UPDATE repos SET archived = TRUE WHERE id = $(repoId)::bigint`, {
        repoId: githubRepoId,
      })
      await rescore([githubRepoId])

      const after = await storedLink(githubRepoId)
      expect(before.confidence - after.confidence).toBeCloseTo(0.2, 9)
    })

    it('is idempotent', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'declared' })
      await qx.result(`UPDATE repos SET is_fork = TRUE WHERE id = $(repoId)::bigint`, {
        repoId: githubRepoId,
      })

      await rescore([githubRepoId])
      const first = await storedLink(githubRepoId)
      await rescore([githubRepoId])
      const second = await storedLink(githubRepoId)

      expect(second.confidence).toBeCloseTo(first.confidence, 9)
    })

    it('skips deps_dev links whose provenance predates the column', async () => {
      await upsertPackageRepo(qx, packageId, githubRepoId, {
        source: 'deps_dev',
        provenance: 'SLSA_ATTESTATION',
      })
      await qx.result(
        `UPDATE package_repos SET provenance = NULL
          WHERE package_id = $(packageId)::bigint AND repo_id = $(repoId)::bigint`,
        { packageId, repoId: githubRepoId },
      )
      const before = await storedLink(githubRepoId)

      await rescore([githubRepoId])

      const after = await storedLink(githubRepoId)
      expect(after.confidence).toBeCloseTo(before.confidence, 9)
    })

    it('rejects a non-positive chunk size', async () => {
      await expect(
        qx.result(`CALL rescore_package_repo_confidence(NULL::bigint[], 0)`, {}),
      ).rejects.toThrow(/chunk_size must be positive/)
    })
  })

  describe('best repo link ordering', () => {
    async function bestRepoId(): Promise<string | null> {
      const row: { repo_id: string | null } = await qx.selectOne(
        `SELECT pr.repo_id::text AS repo_id
           FROM packages p
           ${BEST_REPO_LINK_JOIN}
          WHERE p.id = $(packageId)::bigint`,
        { packageId },
      )
      return row.repo_id
    }

    it('picks the highest-confidence link', async () => {
      await upsertPackageRepo(qx, packageId, gitlabRepoId, { source: 'manual' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'heuristic' })

      expect(await bestRepoId()).toBe(gitlabRepoId)
    })

    it('breaks a confidence tie on the highest repo id', async () => {
      await upsertPackageRepo(qx, packageId, gitlabRepoId, { source: 'declared' })
      await upsertPackageRepo(qx, packageId, githubRepoId, { source: 'declared' })
      await qx.result(
        `UPDATE package_repos SET confidence = 0.85
          WHERE package_id = $(packageId)::bigint`,
        { packageId },
      )

      const higher = BigInt(githubRepoId) > BigInt(gitlabRepoId) ? githubRepoId : gitlabRepoId
      expect(await bestRepoId()).toBe(higher)
    })
  })
})
