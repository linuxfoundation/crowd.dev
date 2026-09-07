import { beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

// Integration test: hits the running packages-db, where V1788307200 defines
// package_repo_confidence. Skipped when the DB env vars are missing so unit-test runs
// in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

type ScoreInput = {
  source: string
  ecosystem?: string
  provenance?: string | null
  archived?: boolean | null
  isFork?: boolean | null
  disabled?: boolean | null
  host?: string | null
  competingGithub?: boolean
  repoId?: number
}

describe.skipIf(!HAVE_DB)('package_repo_confidence', () => {
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
  })

  async function score(input: ScoreInput): Promise<number> {
    const row = await qx.selectOne(
      `SELECT package_repo_confidence(
         $(source), $(ecosystem), $(provenance),
         $(archived), $(isFork), $(disabled), $(host), $(competingGithub), $(repoId)
       )::float8 AS score`,
      {
        ecosystem: 'npm',
        provenance: null,
        archived: null,
        isFork: null,
        disabled: null,
        host: 'github',
        competingGithub: false,
        repoId: 0,
        ...input,
      },
    )
    return row.score as number
  }

  it('scores the source tiers', async () => {
    expect(await score({ source: 'manual' })).toBeCloseTo(0.99, 2)
    expect(await score({ source: 'deps_dev', provenance: 'SLSA_ATTESTATION' })).toBeCloseTo(0.99, 2)
    expect(await score({ source: 'deps_dev', provenance: 'GO_ORIGIN' })).toBeCloseTo(0.9, 2)
    expect(await score({ source: 'deps_dev', provenance: 'UNVERIFIED_METADATA' })).toBeCloseTo(
      0.5,
      2,
    )
    expect(await score({ source: 'declared' })).toBeCloseTo(0.85, 2)
    expect(await score({ source: 'declared', ecosystem: 'maven' })).toBeCloseTo(0.8, 2)
    expect(await score({ source: 'heuristic' })).toBeCloseTo(0.3, 2)
  })

  it('stacks repo-state penalties and floors at 0.05', async () => {
    expect(await score({ source: 'declared', archived: true })).toBeCloseTo(0.65, 2)
    expect(await score({ source: 'declared', isFork: true })).toBeCloseTo(0.75, 2)
    expect(await score({ source: 'declared', archived: true, isFork: true })).toBeCloseTo(0.55, 2)
    expect(await score({ source: 'declared', disabled: true })).toBeCloseTo(0.05, 2)
    expect(
      await score({
        source: 'heuristic',
        archived: true,
        isFork: true,
      }),
    ).toBeCloseTo(0.05, 2)
  })

  it('demotes a non-github link only when a github repo competes', async () => {
    expect(await score({ source: 'declared', host: 'gitlab', competingGithub: true })).toBeCloseTo(
      0.8,
      2,
    )
    expect(await score({ source: 'declared', host: 'gitlab', competingGithub: false })).toBeCloseTo(
      0.85,
      2,
    )
    expect(await score({ source: 'declared', host: 'github', competingGithub: true })).toBeCloseTo(
      0.85,
      2,
    )
  })

  it('breaks ties by source priority then repo id, without crossing a tier', async () => {
    const declared = await score({ source: 'declared', repoId: 10 })
    const otherRepo = await score({ source: 'declared', repoId: 11 })
    expect(declared).not.toBe(otherRepo)
    expect(Math.abs(declared - 0.85)).toBeLessThan(0.004)

    const manual = await score({ source: 'manual', repoId: 10 })
    const attested = await score({
      source: 'deps_dev',
      provenance: 'SLSA_ATTESTATION',
      repoId: 10,
    })
    expect(manual).toBeGreaterThan(attested)
    expect(manual - attested).toBeLessThan(0.004)
  })
})
