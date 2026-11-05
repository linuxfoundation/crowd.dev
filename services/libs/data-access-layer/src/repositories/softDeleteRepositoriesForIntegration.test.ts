import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'
import { insertRepositories, softDeleteRepositoriesForIntegration } from './index'

const test = withQx(base)

async function createIntegration(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), 'github-nango', 'done', $(tenantId), NOW(), NOW())
    `,
    { id, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function createRepositories(
  qx: QueryExecutor,
  sourceIntegrationId: string,
  urls: string[],
): Promise<void> {
  const [segmentGroup] = (
    await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
  ).projectGroups
  const insightsProject = await createInsightsProject(qx, {
    name: generateUUIDv1(),
    slug: generateUUIDv1(),
    isLF: false,
  })
  const gitIntegrationId = await createIntegration(qx)
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
}

async function liveUrls(qx: QueryExecutor, sourceIntegrationId: string): Promise<string[]> {
  const rows: { url: string }[] = await qx.select(
    `SELECT url FROM public.repositories
     WHERE "sourceIntegrationId" = $(sourceIntegrationId) AND "deletedAt" IS NULL
     ORDER BY url`,
    { sourceIntegrationId },
  )
  return rows.map((row) => row.url)
}

describe('softDeleteRepositoriesForIntegration', () => {
  test('soft-deletes only the live repositories of the given integration and counts them', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const otherId = await createIntegration(qx)
    await createRepositories(qx, integrationId, [
      'https://github.com/acme/a',
      'https://github.com/acme/b',
      'https://github.com/acme/gone',
    ])
    await createRepositories(qx, otherId, ['https://github.com/other/c'])
    await qx.result(
      `UPDATE public.repositories SET "deletedAt" = NOW() WHERE url = 'https://github.com/acme/gone'`,
    )

    await expect(softDeleteRepositoriesForIntegration(qx, integrationId)).resolves.toBe(2)
    await expect(liveUrls(qx, integrationId)).resolves.toEqual([])
    await expect(liveUrls(qx, otherId)).resolves.toEqual(['https://github.com/other/c'])
    await expect(softDeleteRepositoriesForIntegration(qx, integrationId)).resolves.toBe(0)
  })
})
