import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'
import { findRepositoryOwnersForSegment, insertRepositories } from '../repositories'
import {
  addGithubNangoConnection,
  addRepoToGitIntegration,
  fetchGithubLifecycleIntegration,
  listGithubChannelsOwnedElsewhere,
  listGithubIntegrationsNeedingReconcile,
  listGithubRepoKeysUnderOwners,
} from './index'

const test = withQx(base)

async function createSegmentId(qx: QueryExecutor): Promise<string> {
  const [segmentGroup] = (
    await createSegments(qx, [{ name: generateUUIDv1(), slug: generateUUIDv1() }])
  ).projectGroups
  return segmentGroup.row.id
}

async function createIntegration(
  qx: QueryExecutor,
  platform: string,
  segmentId: string,
  overrides: { deleted?: boolean; settings?: object } = {},
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "segmentId", settings, "createdAt", "updatedAt", "deletedAt")
    VALUES ($(id), $(platform), 'done', $(tenantId), $(segmentId), $(settings)::jsonb, NOW(), NOW(), $(deletedAt)::timestamptz)
    `,
    {
      id,
      platform,
      tenantId: DEFAULT_TENANT_ID,
      segmentId,
      settings: JSON.stringify(overrides.settings ?? {}),
      deletedAt: overrides.deleted ? new Date().toISOString() : null,
    },
  )
  return id
}

async function createSyncUnit(
  qx: QueryExecutor,
  integrationId: string,
  channelName: string,
  status = 'active',
): Promise<void> {
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status, "emitEnabled")
    VALUES
      ($(id), $(integrationId), 'github-nango', $(channelName), $(channelName), 'issues', $(status), false)
    `,
    { id: generateUUIDv1(), integrationId, channelName, status },
  )
}

async function createLiveRepository(
  qx: QueryExecutor,
  segmentId: string,
  sourceIntegrationId: string,
  url: string,
): Promise<void> {
  const gitIntegrationId = await createIntegration(qx, 'git', segmentId)
  const insightsProject = await createInsightsProject(qx, {
    name: generateUUIDv1(),
    slug: generateUUIDv1(),
    isLF: true,
  })
  await insertRepositories(qx, [
    {
      id: generateUUIDv1(),
      url,
      segmentId,
      gitIntegrationId,
      sourceIntegrationId,
      insightsProjectId: insightsProject.id,
    },
  ])
}

describe('listGithubIntegrationsNeedingReconcile', () => {
  test('lists each reason once and nothing else', async ({ qx }) => {
    const segmentId = await createSegmentId(qx)

    const a = await createIntegration(qx, 'github-nango', segmentId)
    await createLiveRepository(qx, segmentId, a, `https://github.com/acme/${a}`)

    const b = await createIntegration(qx, 'github-nango', segmentId)
    await createSyncUnit(qx, b, `https://github.com/acme/${b}`)

    const c = await createIntegration(qx, 'github-nango', segmentId, { deleted: true })
    await createSyncUnit(qx, c, `https://github.com/acme/${c}`)

    const d = await createIntegration(qx, 'github-nango', segmentId, {
      settings: { orgs: [{ name: 'acme', fullSync: true }] },
    })

    const e = await createIntegration(qx, 'github-nango', segmentId)
    await createLiveRepository(qx, segmentId, e, `https://github.com/Acme/${e}`)
    await addGithubNangoConnection(qx, e, 'conn-e', 'acme', e)

    const f = await createIntegration(qx, 'github-nango', segmentId, { deleted: true })
    await createLiveRepository(qx, segmentId, f, `https://github.com/acme/${f}`)

    const g = await createIntegration(qx, 'github', segmentId)
    await createLiveRepository(qx, segmentId, g, `https://github.com/acme/${g}`)

    const h = await createIntegration(qx, 'github-nango', segmentId)
    await createLiveRepository(qx, segmentId, h, `https://github.com/acme/${h}`)
    await createSyncUnit(qx, h, `https://github.com/acme/${h}`, 'decommissioned')

    const listed = await listGithubIntegrationsNeedingReconcile(qx)

    expect(listed.filter((id) => [a, b, c, d, e, f, g, h].includes(id)).sort()).toEqual(
      [a, b, c, d, f, h].sort(),
    )
    expect(new Set(listed).size).toBe(listed.length)
  })
})

describe('addRepoToGitIntegration', () => {
  test('appends a remote to the segment git integration once', async ({ qx }) => {
    const segmentId = await createSegmentId(qx)
    const github = await createIntegration(qx, 'github-nango', segmentId)
    const git = await createIntegration(qx, 'git', segmentId, {
      settings: { remotes: ['https://github.com/acme/old'] },
    })

    await addRepoToGitIntegration(qx, github, 'https://github.com/acme/New')
    await addRepoToGitIntegration(qx, github, 'https://github.com/acme/New')

    const row = await qx.selectOne(`SELECT settings FROM integrations WHERE id = $(git)`, { git })
    expect(row.settings.remotes).toEqual([
      'https://github.com/acme/old',
      'https://github.com/acme/New',
    ])
  })
})

describe('listGithubRepoKeysUnderOwners', () => {
  test('returns lower-cased repo and mapping keys of live github-nango integrations under the owners', async ({
    qx,
  }) => {
    const segmentId = await createSegmentId(qx)
    const token = generateUUIDv1()
    const owner = `acme-${token}`
    const own = await createIntegration(qx, 'github-nango', segmentId)
    const other = await createIntegration(qx, 'github-nango', segmentId)
    const deleted = await createIntegration(qx, 'github-nango', segmentId, { deleted: true })

    await createLiveRepository(qx, segmentId, own, `https://github.com/Acme-${token}/Own`)
    await addGithubNangoConnection(qx, other, `conn-${token}`, `ACME-${token}`, 'Mapped')
    await createLiveRepository(qx, segmentId, deleted, `https://github.com/acme-${token}/gone`)
    await addGithubNangoConnection(qx, deleted, `conn-d-${token}`, owner, 'gone-mapped')
    await createLiveRepository(qx, segmentId, own, `https://github.com/beta-${token}/elsewhere`)

    const keys = await listGithubRepoKeysUnderOwners(qx, [owner])

    expect(keys.sort()).toEqual([
      `https://github.com/${owner}/mapped`,
      `https://github.com/${owner}/own`,
    ])
    expect(await listGithubRepoKeysUnderOwners(qx, [])).toEqual([])
  })
})

describe('listGithubChannelsOwnedElsewhere', () => {
  test('returns keys held by another live integration via unit or mapping, ignoring this integration and deleted ones', async ({
    qx,
  }) => {
    const segmentId = await createSegmentId(qx)
    const token = generateUUIDv1()
    const url = (name: string) => `https://github.com/acme-${token}/${name}`

    const x = await createIntegration(qx, 'github-nango', segmentId)
    const y = await createIntegration(qx, 'github-nango', segmentId)
    const z = await createIntegration(qx, 'github-nango', segmentId)
    const w = await createIntegration(qx, 'github-nango', segmentId, { deleted: true })

    await createSyncUnit(qx, y, url('one'))
    await addGithubNangoConnection(qx, z, 'conn-z', `Acme-${token}`, 'Two')
    await createSyncUnit(qx, w, url('three'))
    await createSyncUnit(qx, x, url('four'))

    const owned = await listGithubChannelsOwnedElsewhere(qx, x, [
      url('one'),
      url('two'),
      url('three'),
      url('four'),
      url('five'),
    ])

    expect(owned.sort()).toEqual([url('one'), url('two')])
    expect(await listGithubChannelsOwnedElsewhere(qx, x, [])).toEqual([])
  })
})

describe('fetchGithubLifecycleIntegration', () => {
  test('returns deleted integrations too', async ({ qx }) => {
    const segmentId = await createSegmentId(qx)
    const live = await createIntegration(qx, 'github-nango', segmentId, {
      settings: { orgs: [] },
    })
    const deleted = await createIntegration(qx, 'github-nango', segmentId, { deleted: true })

    const liveRow = await fetchGithubLifecycleIntegration(qx, live)
    const deletedRow = await fetchGithubLifecycleIntegration(qx, deleted)

    expect(liveRow).toMatchObject({
      id: live,
      platform: 'github-nango',
      segmentId,
      settings: { orgs: [] },
      deletedAt: null,
    })
    expect(deletedRow?.deletedAt).not.toBeNull()
    expect(await fetchGithubLifecycleIntegration(qx, generateUUIDv1())).toBeNull()
  })
})

describe('findRepositoryOwnersForSegment', () => {
  test('returns the live git integration and insights project of the segment', async ({ qx }) => {
    const segmentId = await createSegmentId(qx)
    await createIntegration(qx, 'git', segmentId, { deleted: true })
    const gitIntegrationId = await createIntegration(qx, 'git', segmentId)
    const insightsProject = await createInsightsProject(qx, {
      name: generateUUIDv1(),
      slug: generateUUIDv1(),
      segmentId,
      isLF: true,
    })

    expect(await findRepositoryOwnersForSegment(qx, segmentId)).toEqual({
      gitIntegrationId,
      insightsProjectId: insightsProject.id,
    })
  })

  test('returns null when the segment lacks a live git integration or insights project', async ({
    qx,
  }) => {
    const withoutProject = await createSegmentId(qx)
    await createIntegration(qx, 'git', withoutProject)

    const withoutGit = await createSegmentId(qx)
    await createInsightsProject(qx, {
      name: generateUUIDv1(),
      slug: generateUUIDv1(),
      segmentId: withoutGit,
      isLF: true,
    })

    expect(await findRepositoryOwnersForSegment(qx, withoutProject)).toBeNull()
    expect(await findRepositoryOwnersForSegment(qx, withoutGit)).toBeNull()
  })
})
