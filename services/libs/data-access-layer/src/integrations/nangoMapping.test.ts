import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { addGithubNangoConnection, getNangoMappingForRepo, listUnmirroredNangoRepos } from './index'

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

describe('listUnmirroredNangoRepos', () => {
  test('returns active mappings without a unit, flags archived repos, and orders by creation', async ({
    qx,
  }) => {
    const [segmentGroup] = (
      await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
    ).projectGroups
    const segmentId = segmentGroup.row.id

    const gitIntegrationId = await createIntegration(qx, 'git', segmentId)
    const nangoIntegrationId = await createIntegration(qx, 'github-nango', segmentId)
    const deletedIntegrationId = await createIntegration(qx, 'github-nango', segmentId)
    const pendingIntegrationId = await createIntegration(qx, 'github-nango', segmentId)
    await qx.result(`UPDATE public.integrations SET status = 'in-progress' WHERE id = $(id)`, {
      id: pendingIntegrationId,
    })
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deletedIntegrationId,
    })

    const insightsProjectId = generateUUIDv1()
    await qx.result(
      `INSERT INTO "insightsProjects" (id, name, slug, "segmentId", "isLF") VALUES ($(id), $(id), $(id), $(segmentId), false)`,
      { id: insightsProjectId, segmentId },
    )
    const archivedRepositoryId = generateUUIDv1()
    await qx.result(
      `
      INSERT INTO public.repositories (id, url, "segmentId", "gitIntegrationId", "sourceIntegrationId", "insightsProjectId", archived)
      VALUES ($(id), $(url), $(segmentId), $(gitIntegrationId), $(nangoIntegrationId), $(insightsProjectId), true)
      `,
      {
        id: archivedRepositoryId,
        url: `https://github.com/${archivedRepositoryId}`,
        segmentId,
        gitIntegrationId,
        nangoIntegrationId,
        insightsProjectId,
      },
    )

    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-mirrored', 'owner', 'repo')
    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-plain', 'plain', 'repo')
    await addGithubNangoConnection(qx, nangoIntegrationId, 'conn-archived', 'old', 'repo')
    await addGithubNangoConnection(qx, deletedIntegrationId, 'conn-deleted', 'gone', 'repo')
    await addGithubNangoConnection(qx, pendingIntegrationId, 'conn-pending', 'wait', 'repo')
    await qx.result(
      `UPDATE integration.nango_mapping SET "repositoryId" = $(archivedRepositoryId) WHERE "connectionId" = 'conn-archived'`,
      { archivedRepositoryId },
    )
    await qx.result(
      `UPDATE integration.nango_mapping SET "createdAt" = NOW() - interval '1 day' WHERE "connectionId" = 'conn-archived'`,
    )
    await qx.result(
      `
      INSERT INTO integration.sync_units (id, "integrationId", platform, "channelId", "channelName", "syncName")
      VALUES ($(id), $(nangoIntegrationId), 'github', 'owner/repo', 'https://github.com/Owner/Repo', 'issues')
      `,
      { id: generateUUIDv1(), nangoIntegrationId },
    )

    const result = await listUnmirroredNangoRepos(qx)
    const ours = result.filter((r) => r.integrationId === nangoIntegrationId)

    expect(
      result.some((r) => [deletedIntegrationId, pendingIntegrationId].includes(r.integrationId)),
    ).toBe(false)
    expect(ours.map((r) => [r.connectionId, r.archived, r.enabled, r.excluded])).toEqual([
      ['conn-archived', true, true, false],
      ['conn-plain', false, true, false],
    ])
  })
})
