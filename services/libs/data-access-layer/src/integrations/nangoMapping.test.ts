import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { addGithubNangoConnection, getNangoMappingForRepo } from './index'

const test = withQx(base)

async function createIntegration(
  qx: QueryExecutor,
  platform: string,
  segmentId: string,
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "segmentId", "createdAt", "updatedAt")
    VALUES ($(id), $(platform), 'done', $(tenantId), $(segmentId), NOW(), NOW())
    `,
    { id, platform, tenantId: DEFAULT_TENANT_ID, segmentId },
  )
  return id
}

describe('getNangoMappingForRepo', () => {
  test('resolves the connectionId via the sibling github-nango integration in the same segment', async ({
    qx,
  }) => {
    const [segmentGroup] = (
      await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
    ).projectGroups
    const segmentId = segmentGroup.row.id

    const githubIntegrationId = await createIntegration(qx, 'github', segmentId)
    const nangoIntegrationId = await createIntegration(qx, 'github-nango', segmentId)
    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-1', 'kubernetes', 'kubernetes')

    const result = await getNangoMappingForRepo(qx, githubIntegrationId, 'kubernetes', 'kubernetes')

    expect(result?.connectionId).toBe('conn-1')
  })

  test('returns null when no github-nango integration exists for the segment', async ({ qx }) => {
    const [segmentGroup] = (
      await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
    ).projectGroups
    const segmentId = segmentGroup.row.id

    const githubIntegrationId = await createIntegration(qx, 'github', segmentId)

    const result = await getNangoMappingForRepo(qx, githubIntegrationId, 'kubernetes', 'kubernetes')

    expect(result).toBeNull()
  })

  test('returns the most recently updated mapping when duplicate-connection cleanup leaves two rows for the same repo', async ({
    qx,
  }) => {
    const [segmentGroup] = (
      await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
    ).projectGroups
    const segmentId = segmentGroup.row.id

    const githubIntegrationId = await createIntegration(qx, 'github', segmentId)
    const nangoIntegrationId = await createIntegration(qx, 'github-nango', segmentId)
    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-old', 'kubernetes', 'kubernetes')
    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-new', 'kubernetes', 'kubernetes')

    const result = await getNangoMappingForRepo(qx, githubIntegrationId, 'kubernetes', 'kubernetes')

    expect(result?.connectionId).toBe('conn-new')
  })

  test('returns null when the repo is not mapped for the sibling integration', async ({ qx }) => {
    const [segmentGroup] = (
      await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
    ).projectGroups
    const segmentId = segmentGroup.row.id

    const githubIntegrationId = await createIntegration(qx, 'github', segmentId)
    const nangoIntegrationId = await createIntegration(qx, 'github-nango', segmentId)
    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-1', 'kubernetes', 'kubernetes')

    const result = await getNangoMappingForRepo(qx, githubIntegrationId, 'torvalds', 'linux')

    expect(result).toBeNull()
  })
})
