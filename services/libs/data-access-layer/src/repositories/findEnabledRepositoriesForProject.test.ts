import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'
import { findEnabledRepositoriesForProject, insertRepositories } from './index'

const test = withQx(base)

async function createIntegration(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), 'github', 'done', $(tenantId), NOW(), NOW())
    `,
    { id, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

interface IRepoSeed {
  url: string
  snapshots?: Array<{ starCount: number; capturedAt: string }>
  excluded?: boolean
  enabled?: boolean
}

async function seedProject(qx: QueryExecutor, repos: IRepoSeed[]): Promise<string> {
  const [segmentGroup] = (
    await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
  ).projectGroups
  const project = await createInsightsProject(qx, {
    name: generateUUIDv1(),
    slug: generateUUIDv1(),
    isLF: false,
  })
  const gitIntegrationId = await createIntegration(qx)
  const sourceIntegrationId = await createIntegration(qx)

  for (const repo of repos) {
    const id = generateUUIDv1()
    await insertRepositories(qx, [
      {
        id,
        url: repo.url,
        segmentId: segmentGroup.row.id,
        gitIntegrationId,
        sourceIntegrationId,
        insightsProjectId: project.id,
        excluded: repo.excluded,
      },
    ])
    if (repo.enabled === false) {
      await qx.result(`UPDATE public.repositories SET enabled = FALSE WHERE id = $(id)`, { id })
    }
    for (const snap of repo.snapshots ?? []) {
      await qx.result(
        `
        INSERT INTO public."repositoryStarSnapshots" ("repositoryId", "starCount", "capturedAt")
        VALUES ($(id), $(starCount), $(capturedAt))
        `,
        { id, ...snap },
      )
    }
  }

  return project.id
}

describe('findEnabledRepositoriesForProject', () => {
  test('orders by latest star count desc, unsnapshotted last, ties by url', async ({ qx }) => {
    const projectId = await seedProject(qx, [
      { url: 'https://github.com/o/none' },
      {
        url: 'https://github.com/o/low',
        snapshots: [{ starCount: 5, capturedAt: '2026-09-01T00:00:00Z' }],
      },
      {
        url: 'https://github.com/o/high',
        snapshots: [{ starCount: 900, capturedAt: '2026-09-01T00:00:00Z' }],
      },
      {
        url: 'https://github.com/o/tie-b',
        snapshots: [{ starCount: 50, capturedAt: '2026-09-01T00:00:00Z' }],
      },
      {
        url: 'https://github.com/o/tie-a',
        snapshots: [{ starCount: 50, capturedAt: '2026-09-01T00:00:00Z' }],
      },
    ])

    const rows = await findEnabledRepositoriesForProject(qx, projectId)

    expect(rows).toEqual([
      { url: 'https://github.com/o/high', starCount: 900 },
      { url: 'https://github.com/o/tie-a', starCount: 50 },
      { url: 'https://github.com/o/tie-b', starCount: 50 },
      { url: 'https://github.com/o/low', starCount: 5 },
      { url: 'https://github.com/o/none', starCount: null },
    ])
  })

  test('uses the latest snapshot, not the maximum one', async ({ qx }) => {
    const projectId = await seedProject(qx, [
      {
        url: 'https://github.com/o/shrunk',
        snapshots: [
          { starCount: 1000, capturedAt: '2026-01-01T00:00:00Z' },
          { starCount: 10, capturedAt: '2026-09-01T00:00:00Z' },
        ],
      },
      {
        url: 'https://github.com/o/steady',
        snapshots: [{ starCount: 100, capturedAt: '2026-09-01T00:00:00Z' }],
      },
    ])

    const rows = await findEnabledRepositoriesForProject(qx, projectId)

    expect(rows.map((r) => r.url)).toEqual([
      'https://github.com/o/steady',
      'https://github.com/o/shrunk',
    ])
    expect(rows[1].starCount).toBe(10)
  })

  test('skips disabled and excluded repos', async ({ qx }) => {
    const projectId = await seedProject(qx, [
      { url: 'https://github.com/o/kept' },
      { url: 'https://github.com/o/disabled', enabled: false },
      { url: 'https://github.com/o/excluded', excluded: true },
    ])

    const rows = await findEnabledRepositoriesForProject(qx, projectId)

    expect(rows.map((r) => r.url)).toEqual(['https://github.com/o/kept'])
  })
})
