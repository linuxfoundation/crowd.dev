import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import { createInsightsProject } from '../collections'
import { createProjectDocOverride, deactivateProjectDocOverride } from '../project-doc-overrides'
import { startDocReadinessRun } from '../project-doc-readiness-runs'
import { QueryExecutor } from '../queryExecutor'
import {
  findLatestProjectDocReadiness,
  findLatestProjectDocReadinessUpdatedAt,
  findProjectDocReadinessChecks,
  findProjectForDocsDiscovery,
  findProjectsForDocsReadiness,
  lockProjectDocReadiness,
  replaceProjectDocReadinessChecks,
  upsertProjectDocReadiness,
} from './projectDocReadiness'
import { IProjectDocReadinessUpsert, NO_DOCS_URL_ERROR } from './types'

const test = withQx(base)

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

async function waitForQueuedAdvisoryLock(qx: QueryExecutor) {
  for (let attempt = 0; attempt < 250; attempt++) {
    const queued = await qx.select(
      `
      SELECT 1
      FROM pg_locks
      WHERE locktype = 'advisory'
        AND NOT granted
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
      `,
    )
    if (queued.length > 0) {
      return
    }
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error('no transaction is queued behind the advisory lock')
}

function scored(projectId: string, over: Partial<IProjectDocReadinessUpsert> = {}) {
  return {
    projectId,
    projectSlug: 'kyverno',
    projectName: 'Kyverno',
    docsUrl: 'https://kyverno.io/docs/',
    discoveryMethod: 'docs-path' as const,
    confidence: 'medium' as const,
    isOverride: false,
    overallScore: 82,
    overallGrade: 'B',
    categoryScores: { llmsTxt: 100, markdown: 50 },
    runId: null,
    durationMs: 1234,
    ok: true,
    error: null,
    ...over,
  }
}

describe('upsertProjectDocReadiness', () => {
  test('inserts a row dated today and returns parsed category scores', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })
    const run = await startDocReadinessRun(qx, { trigger: 'on-demand', scope: 'lf' })

    const row = await upsertProjectDocReadiness(qx, scored(project.id, { runId: run.id }))

    expect(row.projectId).toBe(project.id)
    expect(row.runId).toBe(run.id)
    expect(row.runDate).toBe(new Date().toISOString().slice(0, 10))
    expect(row.overallScore).toBe(82)
    expect(row.categoryScores).toEqual({ llmsTxt: 100, markdown: 50 })
  })

  test('a same-day re-run overwrites the earlier row', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })

    await upsertProjectDocReadiness(qx, scored(project.id, { ok: false, error: 'no-docs-url' }))
    const second = await upsertProjectDocReadiness(qx, scored(project.id))

    expect(second.ok).toBe(true)
    expect(second.error).toBeNull()
    expect(await qx.select(`SELECT 1 FROM "projectDocReadiness"`)).toHaveLength(1)
  })

  test('findLatestProjectDocReadiness picks the most recent runDate', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })

    await upsertProjectDocReadiness(
      qx,
      scored(project.id, { runDate: '2026-01-01', overallScore: 40 }),
    )
    await upsertProjectDocReadiness(
      qx,
      scored(project.id, { runDate: '2026-02-01', overallScore: 70 }),
    )

    const latest = await findLatestProjectDocReadiness(qx, project.id)

    expect(latest?.runDate).toBe('2026-02-01')
    expect(latest?.overallScore).toBe(70)
  })
})

describe('lockProjectDocReadiness', () => {
  test('makes an overlapping transaction on the same project wait for the first to commit', async ({
    qx,
  }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })
    const events: string[] = []
    const firstHoldsLock = deferred()
    const firstMayCommit = deferred()

    const first = qx.tx(async (tx) => {
      await lockProjectDocReadiness(tx, project.id)
      firstHoldsLock.resolve()
      await firstMayCommit.promise
      events.push('first commits')
    })
    await firstHoldsLock.promise

    const second = qx.tx(async (tx) => {
      await lockProjectDocReadiness(tx, project.id)
      events.push('second holds lock')
    })
    try {
      await waitForQueuedAdvisoryLock(qx)
    } finally {
      firstMayCommit.resolve()
    }
    await Promise.all([first, second])

    expect(events).toEqual(['first commits', 'second holds lock'])
  })

  test('does not block a transaction on another project', async ({ qx }) => {
    const a = await createInsightsProject(qx, { name: 'A', slug: 'a', isLF: true })
    const b = await createInsightsProject(qx, { name: 'B', slug: 'b', isLF: true })

    await qx.tx(async (txA) => {
      await lockProjectDocReadiness(txA, a.id)
      await qx.tx(async (txB) => {
        await txB.selectNone(`SET LOCAL lock_timeout = '2s'`)
        await lockProjectDocReadiness(txB, b.id)
      })
    })
  })
})

describe('replaceProjectDocReadinessChecks', () => {
  test('replaces the previous check rows for the project only', async ({ qx }) => {
    const a = await createInsightsProject(qx, { name: 'A', slug: 'a', isLF: true })
    const b = await createInsightsProject(qx, { name: 'B', slug: 'b', isLF: true })
    const check = (checkId: string, status: 'pass' | 'fail') => ({
      checkId,
      category: 'llmsTxt',
      status,
      message: null,
      details: null,
      durationMs: 10,
    })

    await replaceProjectDocReadinessChecks(qx, a.id, [
      check('llms-txt', 'fail'),
      check('old', 'pass'),
    ])
    await replaceProjectDocReadinessChecks(qx, b.id, [check('llms-txt', 'pass')])
    await replaceProjectDocReadinessChecks(qx, a.id, [check('llms-txt', 'pass')])

    const forA = await findProjectDocReadinessChecks(qx, a.id)
    expect(forA.map((c) => [c.checkId, c.status])).toEqual([['llms-txt', 'pass']])
    expect(forA[0].scoredAt).not.toBeNull()
    expect(await findProjectDocReadinessChecks(qx, b.id)).toHaveLength(1)
  })

  test('an empty list clears the rows', async ({ qx }) => {
    const a = await createInsightsProject(qx, { name: 'A', slug: 'a', isLF: true })
    await replaceProjectDocReadinessChecks(qx, a.id, [
      {
        checkId: 'x',
        category: 'c',
        status: 'pass',
        message: null,
        details: null,
        durationMs: null,
      },
    ])

    await replaceProjectDocReadinessChecks(qx, a.id, [])

    expect(await findProjectDocReadinessChecks(qx, a.id)).toHaveLength(0)
  })
})

describe('findLatestProjectDocReadinessUpdatedAt', () => {
  test('is null when nothing has been written', async ({ qx }) => {
    expect(await findLatestProjectDocReadinessUpdatedAt(qx)).toBeNull()
  })

  test('returns the newest updatedAt, including failure rows', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })
    await upsertProjectDocReadiness(qx, scored(project.id, { runDate: '2026-02-01' }))
    await qx.result(`UPDATE "projectDocReadiness" SET "updatedAt" = '2026-01-01T00:00:00Z'`)
    await upsertProjectDocReadiness(
      qx,
      scored(project.id, { runDate: '2026-02-02', ok: false, error: 'no-docs-url' }),
    )

    const latest = await findLatestProjectDocReadinessUpdatedAt(qx)

    expect(latest).toBeInstanceOf(Date)
    expect(latest!.getTime()).toBeGreaterThan(new Date('2026-01-02').getTime())
  })
})

describe('findProjectsForDocsReadiness', () => {
  test('full lf sweep returns enabled, live LF projects ordered by id', async ({ qx }) => {
    const lf = await createInsightsProject(qx, { name: 'LF', slug: 'lf', isLF: true })
    await createInsightsProject(qx, { name: 'Non LF', slug: 'non-lf', isLF: false })
    await createInsightsProject(qx, { name: 'Disabled', slug: 'off', isLF: true, enabled: false })
    const deleted = await createInsightsProject(qx, { name: 'Gone', slug: 'gone', isLF: true })
    await qx.selectNone(`UPDATE "insightsProjects" SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deleted.id,
    })

    const rows = await findProjectsForDocsReadiness(qx, { mode: 'full', scope: 'lf', limit: 10 })

    expect(rows).toEqual([{ id: lf.id, slug: 'lf', name: 'LF' }])
  })

  test('scope all includes non-LF projects', async ({ qx }) => {
    await createInsightsProject(qx, { name: 'LF', slug: 'lf', isLF: true })
    await createInsightsProject(qx, { name: 'Non LF', slug: 'non-lf', isLF: false })

    const rows = await findProjectsForDocsReadiness(qx, { mode: 'full', scope: 'all', limit: 10 })

    expect(rows).toHaveLength(2)
  })

  test('incremental picks unscored projects and projects whose latest row failed', async ({
    qx,
  }) => {
    const unscored = await createInsightsProject(qx, { name: 'U', slug: 'u', isLF: true })
    const failed = await createInsightsProject(qx, { name: 'F', slug: 'f', isLF: true })
    const recovered = await createInsightsProject(qx, { name: 'R', slug: 'r', isLF: true })
    const fine = await createInsightsProject(qx, { name: 'OK', slug: 'ok', isLF: true })

    await upsertProjectDocReadiness(qx, scored(failed.id, { ok: false, error: 'boom' }))
    await upsertProjectDocReadiness(qx, scored(recovered.id, { runDate: '2026-01-01', ok: false }))
    await upsertProjectDocReadiness(qx, scored(recovered.id, { runDate: '2026-01-02', ok: true }))
    await upsertProjectDocReadiness(qx, scored(fine.id))

    const rows = await findProjectsForDocsReadiness(qx, {
      mode: 'incremental',
      scope: 'lf',
      limit: 10,
    })

    expect(rows.map((r) => r.id).sort()).toEqual([unscored.id, failed.id].sort())
  })

  test('incremental skips latest no-docs-url unless an active override exists', async ({ qx }) => {
    const unscored = await createInsightsProject(qx, { name: 'A', slug: 'a', isLF: true })
    const fine = await createInsightsProject(qx, { name: 'B', slug: 'b', isLF: true })
    const timedOut = await createInsightsProject(qx, { name: 'C', slug: 'c', isLF: true })
    const noUrl = await createInsightsProject(qx, { name: 'D', slug: 'd', isLF: true })
    const noUrlOverride = await createInsightsProject(qx, { name: 'E', slug: 'e', isLF: true })
    const noUrlOldOk = await createInsightsProject(qx, { name: 'F', slug: 'f', isLF: true })
    const noUrlInactive = await createInsightsProject(qx, { name: 'G', slug: 'g', isLF: true })
    const noUrlThenTimeout = await createInsightsProject(qx, { name: 'H', slug: 'h', isLF: true })

    const noUrlRow = { ok: false, error: NO_DOCS_URL_ERROR }
    await upsertProjectDocReadiness(qx, scored(fine.id))
    await upsertProjectDocReadiness(qx, scored(timedOut.id, { ok: false, error: 'timeout' }))
    await upsertProjectDocReadiness(qx, scored(noUrl.id, noUrlRow))
    await upsertProjectDocReadiness(qx, scored(noUrlOverride.id, noUrlRow))
    await upsertProjectDocReadiness(qx, scored(noUrlOldOk.id, { runDate: '2026-01-01' }))
    await upsertProjectDocReadiness(
      qx,
      scored(noUrlOldOk.id, { runDate: '2026-01-02', ...noUrlRow }),
    )
    await upsertProjectDocReadiness(qx, scored(noUrlInactive.id, noUrlRow))
    await upsertProjectDocReadiness(
      qx,
      scored(noUrlThenTimeout.id, { runDate: '2026-01-01', ...noUrlRow }),
    )
    await upsertProjectDocReadiness(
      qx,
      scored(noUrlThenTimeout.id, { runDate: '2026-01-02', ok: false, error: 'timeout' }),
    )

    const override = (projectId: string) => ({
      projectId,
      docsUrl: 'https://example.com/docs',
      submittedBy: 'test',
    })
    await createProjectDocOverride(qx, override(noUrlOverride.id))
    await createProjectDocOverride(qx, override(noUrlInactive.id))
    await deactivateProjectDocOverride(qx, noUrlInactive.id)

    const incremental = await findProjectsForDocsReadiness(qx, {
      mode: 'incremental',
      scope: 'lf',
      limit: 20,
    })
    const full = await findProjectsForDocsReadiness(qx, { mode: 'full', scope: 'lf', limit: 20 })

    expect(incremental.map((r) => r.id).sort()).toEqual(
      [unscored.id, timedOut.id, noUrlOverride.id, noUrlThenTimeout.id].sort(),
    )
    expect(full).toHaveLength(8)
  })

  test('pages by id with afterId and limit', async ({ qx }) => {
    for (const slug of ['a', 'b', 'c']) {
      await createInsightsProject(qx, { name: slug, slug, isLF: true })
    }

    const first = await findProjectsForDocsReadiness(qx, { mode: 'full', scope: 'lf', limit: 2 })
    const rest = await findProjectsForDocsReadiness(qx, {
      mode: 'full',
      scope: 'lf',
      afterId: first[1].id,
      limit: 2,
    })

    expect(first).toHaveLength(2)
    expect(rest).toHaveLength(1)
    expect(rest[0].id > first[1].id).toBe(true)
  })
})

const sharedCount = { withWebsiteSharedCount: true } as const

describe('findProjectForDocsDiscovery websiteSharedCount', () => {
  test('counts live enabled siblings sharing the website, ignoring scheme, www and trailing slash', async ({
    qx,
  }) => {
    const a = await createInsightsProject(qx, {
      name: 'A',
      slug: 'a',
      isLF: true,
      website: 'https://foundation.org/projects/x/',
    })
    await createInsightsProject(qx, {
      name: 'B',
      slug: 'b',
      isLF: true,
      website: 'http://www.foundation.org/projects/x',
    })
    await createInsightsProject(qx, {
      name: 'W',
      slug: 'w',
      isLF: true,
      website: 'https://wwwXfoundation.org/projects/x',
    })
    await createInsightsProject(qx, {
      name: 'C',
      slug: 'c',
      isLF: true,
      website: 'https://foundation.org/y',
    })

    expect((await findProjectForDocsDiscovery(qx, a.id, sharedCount))?.websiteSharedCount).toBe(1)
  })

  test('is 0 for a unique website and for a missing website', async ({ qx }) => {
    const unique = await createInsightsProject(qx, {
      name: 'U',
      slug: 'u',
      isLF: true,
      website: 'https://unique.org',
    })
    const none = await createInsightsProject(qx, { name: 'N', slug: 'n', isLF: true })
    await createInsightsProject(qx, { name: 'N2', slug: 'n2', isLF: true })

    expect(
      (await findProjectForDocsDiscovery(qx, unique.id, sharedCount))?.websiteSharedCount,
    ).toBe(0)
    expect((await findProjectForDocsDiscovery(qx, none.id, sharedCount))?.websiteSharedCount).toBe(
      0,
    )
  })

  test('does not count deleted or disabled siblings', async ({ qx }) => {
    const a = await createInsightsProject(qx, {
      name: 'A',
      slug: 'a',
      isLF: true,
      website: 'https://s.org',
    })
    const gone = await createInsightsProject(qx, {
      name: 'G',
      slug: 'g',
      isLF: true,
      website: 'https://s.org',
    })
    await createInsightsProject(qx, {
      name: 'D',
      slug: 'd',
      isLF: true,
      website: 'https://s.org',
      enabled: false,
    })
    await qx.selectNone(`UPDATE "insightsProjects" SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: gone.id,
    })

    expect((await findProjectForDocsDiscovery(qx, a.id, sharedCount))?.websiteSharedCount).toBe(0)
  })

  test('matches schemeless www websites against their https form in both directions', async ({
    qx,
  }) => {
    const schemeless = await createInsightsProject(qx, {
      name: 'S',
      slug: 's',
      isLF: true,
      website: 'www.foundation.org/projects/x',
    })
    const https = await createInsightsProject(qx, {
      name: 'H',
      slug: 'h',
      isLF: true,
      website: 'https://foundation.org/projects/x',
    })

    expect(
      (await findProjectForDocsDiscovery(qx, schemeless.id, sharedCount))?.websiteSharedCount,
    ).toBe(1)
    expect((await findProjectForDocsDiscovery(qx, https.id, sharedCount))?.websiteSharedCount).toBe(
      1,
    )
  })

  test('does not strip a schemeless host that merely starts with www', async ({ qx }) => {
    const a = await createInsightsProject(qx, {
      name: 'A',
      slug: 'a',
      isLF: true,
      website: 'foundation.org/projects/x',
    })
    await createInsightsProject(qx, {
      name: 'W',
      slug: 'w',
      isLF: true,
      website: 'wwwXfoundation.org/projects/x',
    })

    expect((await findProjectForDocsDiscovery(qx, a.id, sharedCount))?.websiteSharedCount).toBe(0)
  })

  test('skips the sibling count unless asked for it', async ({ qx }) => {
    const a = await createInsightsProject(qx, {
      name: 'A',
      slug: 'a',
      isLF: true,
      website: 'https://s.org',
    })
    await createInsightsProject(qx, { name: 'B', slug: 'b', isLF: true, website: 'https://s.org' })

    const plain = await findProjectForDocsDiscovery(qx, a.id)

    expect(plain).toEqual({ id: a.id, slug: 'a', name: 'A', website: 'https://s.org' })
    expect((await findProjectForDocsDiscovery(qx, a.id, sharedCount))?.websiteSharedCount).toBe(1)
  })
})
