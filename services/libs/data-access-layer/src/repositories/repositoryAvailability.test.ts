import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'

import { createInsightsProject } from '../collections'
import {
  clearRepositoriesUnavailable,
  insertRepositories,
  listUnavailableRepoUrlsForIntegration,
  markRepositoriesUnavailable,
} from './index'

const test = withQx(base)

const GONE = 'https://github.com/acme/Gone'
const LIVE = 'https://github.com/acme/live'
const REMOVED = 'https://github.com/acme/removed'

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

async function createRepositories(qx: QueryExecutor, sourceIntegrationId: string): Promise<void> {
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
    [GONE, LIVE, REMOVED].map((url) => ({
      id: generateUUIDv1(),
      url,
      segmentId: segmentGroup.row.id,
      gitIntegrationId,
      sourceIntegrationId,
      insightsProjectId: insightsProject.id,
    })),
  )
  await qx.result(
    `UPDATE public.repositories
     SET "deletedAt" = CASE WHEN url = $(removed) THEN NOW() END,
         "updatedAt" = NOW() - INTERVAL '1 day'`,
    { removed: REMOVED },
  )
  await qx.result(
    `UPDATE git."repositoryProcessing" rp SET state = 'auth_required', "lockedAt" = NOW()
     FROM public.repositories r WHERE r.id = rp."repositoryId" AND r.url = $(gone)`,
    { gone: GONE },
  )
}

interface IRepoState {
  url: string
  unavailable: boolean
  touched: boolean
  state: string
}

async function repoStates(qx: QueryExecutor): Promise<IRepoState[]> {
  return qx.select(
    `SELECT r.url,
            r."unavailableAt" IS NOT NULL AS unavailable,
            r."updatedAt" > NOW() - INTERVAL '1 hour' AS touched,
            rp.state
     FROM public.repositories r
     JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id
     ORDER BY r.url`,
  )
}

describe('repository availability', () => {
  test('flags and unflags only live repositories, bumps updatedAt once and wakes git processing', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    await createRepositories(qx, integrationId)

    await expect(
      markRepositoriesUnavailable(qx, [GONE, REMOVED, GONE.toLowerCase()]),
    ).resolves.toEqual([GONE])
    await expect(markRepositoriesUnavailable(qx, [GONE])).resolves.toEqual([])
    await expect(listUnavailableRepoUrlsForIntegration(qx, integrationId)).resolves.toEqual([GONE])
    await expect(repoStates(qx)).resolves.toEqual([
      { url: GONE, unavailable: true, touched: true, state: 'auth_required' },
      { url: LIVE, unavailable: false, touched: false, state: 'pending' },
      { url: REMOVED, unavailable: false, touched: false, state: 'pending' },
    ])

    await expect(clearRepositoriesUnavailable(qx, [GONE, LIVE])).resolves.toEqual([GONE])
    await expect(clearRepositoriesUnavailable(qx, [GONE])).resolves.toEqual([])
    await expect(listUnavailableRepoUrlsForIntegration(qx, integrationId)).resolves.toEqual([])
    await expect(repoStates(qx)).resolves.toMatchObject([
      { url: GONE, unavailable: false, touched: true, state: 'pending' },
      { url: LIVE, touched: false },
      { url: REMOVED, touched: false },
    ])
  })
})
