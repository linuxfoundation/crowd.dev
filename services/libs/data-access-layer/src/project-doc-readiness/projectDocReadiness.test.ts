import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import { createInsightsProject } from '../collections'
import { startDocReadinessRun } from '../project-doc-readiness-runs'

import {
  findLatestProjectDocReadiness,
  findProjectDocReadinessChecks,
  findProjectsForDocsReadiness,
  replaceProjectDocReadinessChecks,
  upsertProjectDocReadiness,
} from './projectDocReadiness'
import { IProjectDocReadinessUpsert } from './types'

const test = withQx(base)

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
