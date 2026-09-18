import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'

import { findGithubOwnersWithNonLfRepos, insertRepositories } from './index'

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
  overrides: {
    url: string
    isLF?: boolean
    repoDeletedAt?: string | null
    projectDeletedAt?: string | null
  },
): Promise<void> {
  const [segmentGroup] = (
    await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
  ).projectGroups
  const gitIntegrationId = await createIntegration(qx)
  const sourceIntegrationId = await createIntegration(qx)

  const insightsProject = await createInsightsProject(qx, {
    name: generateUUIDv1(),
    slug: generateUUIDv1(),
    isLF: overrides.isLF ?? false,
  })
  const insightsProjectId = insightsProject.id

  if (overrides.projectDeletedAt) {
    await qx.result(`UPDATE "insightsProjects" SET "deletedAt" = $(deletedAt) WHERE id = $(id)`, {
      deletedAt: overrides.projectDeletedAt,
      id: insightsProjectId,
    })
  }

  await insertRepositories(qx, [
    {
      id: generateUUIDv1(),
      url: overrides.url,
      segmentId: segmentGroup.row.id,
      gitIntegrationId,
      sourceIntegrationId,
      insightsProjectId,
    },
  ])

  if (overrides.repoDeletedAt) {
    await qx.result(
      `UPDATE public.repositories SET "deletedAt" = $(deletedAt) WHERE url = $(url)`,
      {
        deletedAt: overrides.repoDeletedAt,
        url: overrides.url,
      },
    )
  }
}

describe('findGithubOwnersWithNonLfRepos', () => {
  test('returns an empty set for an empty input', async ({ qx }) => {
    const result = await findGithubOwnersWithNonLfRepos(qx, [])
    expect(result.size).toBe(0)
  })

  test('does not match an owner whose repos are all mapped to LF projects', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/kubernetes/kubernetes', isLF: true })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['kubernetes'])

    expect(result.size).toBe(0)
  })

  test('matches an owner with a repo mapped to a non-LF project', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/some-owner/some-repo', isLF: false })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['some-owner'])

    expect(result.has('some-owner')).toBe(true)
  })

  test('treats a repo whose LF project is soft-deleted as non-LF evidence', async ({ qx }) => {
    await createRepository(qx, {
      url: 'https://github.com/torvalds/linux',
      isLF: true,
      projectDeletedAt: new Date().toISOString(),
    })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['torvalds'])

    expect(result.has('torvalds')).toBe(true)
  })

  test('does not match a soft-deleted repo', async ({ qx }) => {
    await createRepository(qx, {
      url: 'https://github.com/gerritcodereview/gerrit',
      isLF: false,
      repoDeletedAt: new Date().toISOString(),
    })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['gerritcodereview'])

    expect(result.size).toBe(0)
  })

  test('does not match a repo stored on a non-GitHub host', async ({ qx }) => {
    await createRepository(qx, { url: 'https://gitlab.com/gitlab-org/gitlab', isLF: false })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['gitlab-org'])

    expect(result.size).toBe(0)
  })

  test('does not leak owners outside the requested batch', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/some-owner/some-repo', isLF: false })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['torvalds'])

    expect(result.size).toBe(0)
  })

  test('matches regardless of the case of the requested owner', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/Some-Owner/some-repo', isLF: false })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['SOME-OWNER'])

    expect(result.has('some-owner')).toBe(true)
  })

  test('does not match an owner-only URL with no repository path segment', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/torvalds', isLF: false })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['torvalds'])

    expect(result.size).toBe(0)
  })

  test('matches a repo stored as an scp-style git@ URL', async ({ qx }) => {
    await createRepository(qx, { url: 'git@github.com:some-owner/some-repo.git', isLF: false })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['some-owner'])

    expect(result.has('some-owner')).toBe(true)
  })

  test('matches a repo stored as an ssh:// URL', async ({ qx }) => {
    await createRepository(qx, {
      url: 'ssh://git@github.com/some-owner/some-repo.git',
      isLF: false,
    })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['some-owner'])

    expect(result.has('some-owner')).toBe(true)
  })

  test('matches a repo stored as an ssh:// URL with a non-standard port', async ({ qx }) => {
    await createRepository(qx, {
      url: 'ssh://git@github.com:2222/some-owner/some-repo.git',
      isLF: false,
    })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['some-owner'])

    expect(result.has('some-owner')).toBe(true)
  })

  test('matches a repo stored as an ssh:// URL with a colon and no port', async ({ qx }) => {
    await createRepository(qx, {
      url: 'ssh://git@github.com:some-owner/some-repo.git',
      isLF: false,
    })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['some-owner'])

    expect(result.has('some-owner')).toBe(true)
  })

  test('matches a repo stored with no scheme at all', async ({ qx }) => {
    await createRepository(qx, {
      url: 'github.com/some-owner/some-repo',
      isLF: false,
    })

    const result = await findGithubOwnersWithNonLfRepos(qx, ['some-owner'])

    expect(result.has('some-owner')).toBe(true)
  })
})
