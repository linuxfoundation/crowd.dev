import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'

import { findRepoUrlsInCdp, insertRepositories } from './index'

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

async function createRepository(
  qx: QueryExecutor,
  overrides: { url: string; deletedAt?: string | null },
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
  const sourceIntegrationId = await createIntegration(qx)

  await insertRepositories(qx, [
    {
      id: generateUUIDv1(),
      url: overrides.url,
      segmentId: segmentGroup.row.id,
      gitIntegrationId,
      sourceIntegrationId,
      insightsProjectId: insightsProject.id,
    },
  ])

  if (overrides.deletedAt) {
    await qx.result(
      `UPDATE public.repositories SET "deletedAt" = $(deletedAt) WHERE url = $(url)`,
      {
        deletedAt: overrides.deletedAt,
        url: overrides.url,
      },
    )
  }
}

describe('findRepoUrlsInCdp', () => {
  test('returns an empty set for an empty input', async ({ qx }) => {
    const result = await findRepoUrlsInCdp(qx, [])
    expect(result.size).toBe(0)
  })

  test('matches a repo stored with the exact canonical URL', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/kubernetes/kubernetes' })

    const result = await findRepoUrlsInCdp(qx, ['https://github.com/kubernetes/kubernetes'])

    expect(result.has('https://github.com/kubernetes/kubernetes')).toBe(true)
  })

  test('matches a repo stored with uppercase owner, .git suffix, trailing slash and www.', async ({
    qx,
  }) => {
    await createRepository(qx, { url: 'https://www.GitHub.com/Kubernetes/Kubernetes.git/' })

    const result = await findRepoUrlsInCdp(qx, ['https://github.com/kubernetes/kubernetes'])

    expect(result.has('https://github.com/kubernetes/kubernetes')).toBe(true)
  })

  test('does not match a soft-deleted repo', async ({ qx }) => {
    await createRepository(qx, {
      url: 'https://github.com/gerritcodereview/gerrit',
      deletedAt: new Date().toISOString(),
    })

    const result = await findRepoUrlsInCdp(qx, ['https://github.com/gerritcodereview/gerrit'])

    expect(result.has('https://github.com/gerritcodereview/gerrit')).toBe(false)
  })

  test('does not match a repo stored on a non-GitHub host', async ({ qx }) => {
    await createRepository(qx, { url: 'https://gitlab.com/gitlab-org/gitlab' })

    const result = await findRepoUrlsInCdp(qx, ['https://github.com/gitlab-org/gitlab'])

    expect(result.has('https://github.com/gitlab-org/gitlab')).toBe(false)
  })

  test('does not include a candidate URL that is not tracked in CDP', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/kubernetes/kubernetes' })

    const result = await findRepoUrlsInCdp(qx, ['https://github.com/torvalds/linux'])

    expect(result.size).toBe(0)
  })
})
