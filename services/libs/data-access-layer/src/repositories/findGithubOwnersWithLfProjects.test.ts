import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'
import { findGithubOwnersWithLfProjects, insertRepositories } from './index'

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
  const insightsProject = await createInsightsProject(qx, {
    name: generateUUIDv1(),
    slug: generateUUIDv1(),
    isLF: overrides.isLF ?? true,
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

  if (overrides.repoDeletedAt) {
    await qx.result(
      `UPDATE public.repositories SET "deletedAt" = $(deletedAt) WHERE url = $(url)`,
      {
        deletedAt: overrides.repoDeletedAt,
        url: overrides.url,
      },
    )
  }

  if (overrides.projectDeletedAt) {
    await qx.result(`UPDATE "insightsProjects" SET "deletedAt" = $(deletedAt) WHERE id = $(id)`, {
      deletedAt: overrides.projectDeletedAt,
      id: insightsProject.id,
    })
  }
}

describe('findGithubOwnersWithLfProjects', () => {
  test('returns an empty set for an empty input', async ({ qx }) => {
    const result = await findGithubOwnersWithLfProjects(qx, [])
    expect(result.size).toBe(0)
  })

  test('matches an owner with a repo mapped to an LF project', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/kubernetes/kubernetes', isLF: true })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('matches regardless of the case of the requested owner', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/kubernetes/kubernetes', isLF: true })

    const result = await findGithubOwnersWithLfProjects(qx, ['Kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('does not match an owner whose repos are only on non-LF projects', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/some-owner/some-repo', isLF: false })

    const result = await findGithubOwnersWithLfProjects(qx, ['some-owner'])

    expect(result.size).toBe(0)
  })

  test('matches a repo stored with uppercase owner, .git suffix, trailing slash and www.', async ({
    qx,
  }) => {
    await createRepository(qx, {
      url: 'https://www.GitHub.com/Kubernetes/Kubernetes.git/',
      isLF: true,
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('does not match a soft-deleted repo', async ({ qx }) => {
    await createRepository(qx, {
      url: 'https://github.com/gerritcodereview/gerrit',
      isLF: true,
      repoDeletedAt: new Date().toISOString(),
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['gerritcodereview'])

    expect(result.size).toBe(0)
  })

  test('does not match a soft-deleted insights project', async ({ qx }) => {
    await createRepository(qx, {
      url: 'https://github.com/torvalds/linux',
      isLF: true,
      projectDeletedAt: new Date().toISOString(),
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['torvalds'])

    expect(result.size).toBe(0)
  })

  test('does not leak owners outside the requested batch', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/kubernetes/kubernetes', isLF: true })

    const result = await findGithubOwnersWithLfProjects(qx, ['torvalds'])

    expect(result.size).toBe(0)
  })

  test('does not match a repo on a non-GitHub host under an LF project', async ({ qx }) => {
    await createRepository(qx, { url: 'https://gitlab.com/gitlab-org/gitlab', isLF: true })

    const result = await findGithubOwnersWithLfProjects(qx, ['gitlab-org'])

    expect(result.size).toBe(0)
  })

  test('does not match an owner-only URL with no repository path segment', async ({ qx }) => {
    await createRepository(qx, { url: 'https://github.com/torvalds', isLF: true })

    const result = await findGithubOwnersWithLfProjects(qx, ['torvalds'])

    expect(result.size).toBe(0)
  })

  test('matches a repo stored as an scp-style git@ URL', async ({ qx }) => {
    await createRepository(qx, { url: 'git@github.com:kubernetes/kubernetes.git', isLF: true })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('matches a repo stored as an ssh:// URL', async ({ qx }) => {
    await createRepository(qx, {
      url: 'ssh://git@github.com/kubernetes/kubernetes.git',
      isLF: true,
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('matches a repo stored as an ssh:// URL with a non-standard port', async ({ qx }) => {
    await createRepository(qx, {
      url: 'ssh://git@github.com:2222/kubernetes/kubernetes.git',
      isLF: true,
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('matches a repo stored as an ssh:// URL with a colon and no port', async ({ qx }) => {
    await createRepository(qx, {
      url: 'ssh://git@github.com:kubernetes/kubernetes.git',
      isLF: true,
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })

  test('matches a repo stored with no scheme at all', async ({ qx }) => {
    await createRepository(qx, {
      url: 'github.com/kubernetes/kubernetes',
      isLF: true,
    })

    const result = await findGithubOwnersWithLfProjects(qx, ['kubernetes'])

    expect(result.has('kubernetes')).toBe(true)
  })
})
