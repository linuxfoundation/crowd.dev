import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'
import { countRepositoriesBySegment, insertRepositories } from './index'

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

async function createSegmentWithRepositories(
  qx: QueryExecutor,
  urls: string[],
  { deleted = [] }: { deleted?: string[] } = {},
): Promise<string> {
  const [segmentGroup] = (
    await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
  ).projectGroups
  const insightsProject = await createInsightsProject(qx, {
    name: generateUUIDv1(),
    slug: generateUUIDv1(),
    isLF: false,
  })
  const gitIntegrationId = await createIntegration(qx)
  const sourceIntegrationId = await createIntegration(qx)

  await insertRepositories(
    qx,
    urls.map((url) => ({
      id: generateUUIDv1(),
      url,
      segmentId: segmentGroup.row.id,
      gitIntegrationId,
      sourceIntegrationId,
      insightsProjectId: insightsProject.id,
    })),
  )
  for (const url of deleted) {
    await qx.result(`UPDATE public.repositories SET "deletedAt" = NOW() WHERE url = $(url)`, {
      url,
    })
  }
  return segmentGroup.row.id
}

describe('countRepositoriesBySegment', () => {
  test('returns an empty map for an empty input', async ({ qx }) => {
    expect((await countRepositoriesBySegment(qx, [])).size).toBe(0)
  })

  test('counts live repositories per segment and leaves out deleted ones', async ({ qx }) => {
    const first = await createSegmentWithRepositories(
      qx,
      ['https://github.com/acme/one', 'https://github.com/acme/two', 'https://github.com/acme/old'],
      { deleted: ['https://github.com/acme/old'] },
    )
    const second = await createSegmentWithRepositories(qx, [])

    const counts = await countRepositoriesBySegment(qx, [first, second])

    expect(counts.get(first)).toBe(2)
    expect(counts.get(second)).toBeUndefined()
  })
})
