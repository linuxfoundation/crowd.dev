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
         "updatedAt" = CASE WHEN url = $(gone) THEN NOW() + INTERVAL '1 day'
                            ELSE NOW() - INTERVAL '1 day' END`,
    { removed: REMOVED, gone: GONE },
  )
  await qx.result(
    `UPDATE git."repositoryProcessing" rp SET state = 'auth_required', "lockedAt" = NOW()
     FROM public.repositories r WHERE r.id = rp."repositoryId" AND r.url = $(gone)`,
    { gone: GONE },
  )
}

interface IRepoState {
  unavailableAt: string | null
  updatedAtMicros: string
  state: string
  lockedAt: string | null
}

async function repoStates(qx: QueryExecutor): Promise<Record<string, IRepoState>> {
  const rows: (IRepoState & { url: string })[] = await qx.select(
    `SELECT r.url,
            r."unavailableAt",
            round(extract(epoch FROM r."updatedAt") * 1000000)::text AS "updatedAtMicros",
            rp.state,
            rp."lockedAt"
     FROM public.repositories r
     JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id`,
  )
  return Object.fromEntries(rows.map(({ url, ...state }) => [url, state]))
}

const ONE_MILLISECOND_IN_MICROS = 1000

describe('repository availability', () => {
  test('flags and unflags only live repositories of the given integration, advances updatedAt on each change only and wakes git processing', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    await createRepositories(qx, integrationId)
    const otherId = await createIntegration(qx)
    const initial = await repoStates(qx)
    const futureVersion = Number(initial[GONE].updatedAtMicros)

    await expect(markRepositoriesUnavailable(qx, otherId, [GONE])).resolves.toEqual([])
    await expect(
      markRepositoriesUnavailable(qx, integrationId, [GONE, REMOVED, GONE.toLowerCase()]),
    ).resolves.toEqual([GONE])
    const marked = await repoStates(qx)
    expect(marked).toEqual({
      ...initial,
      [GONE]: {
        ...initial[GONE],
        unavailableAt: expect.any(String),
        updatedAtMicros: String(futureVersion + ONE_MILLISECOND_IN_MICROS),
      },
    })
    await expect(markRepositoriesUnavailable(qx, integrationId, [GONE])).resolves.toEqual([])
    await expect(clearRepositoriesUnavailable(qx, otherId, [GONE])).resolves.toEqual([])
    await expect(repoStates(qx)).resolves.toEqual(marked)
    await expect(listUnavailableRepoUrlsForIntegration(qx, integrationId)).resolves.toEqual([GONE])

    await expect(clearRepositoriesUnavailable(qx, integrationId, [GONE, LIVE])).resolves.toEqual([
      GONE,
    ])
    const cleared = await repoStates(qx)
    expect(cleared).toEqual({
      ...initial,
      [GONE]: {
        unavailableAt: null,
        updatedAtMicros: String(futureVersion + 2 * ONE_MILLISECOND_IN_MICROS),
        state: 'pending',
        lockedAt: null,
      },
    })
    await expect(clearRepositoriesUnavailable(qx, integrationId, [GONE])).resolves.toEqual([])
    await expect(repoStates(qx)).resolves.toEqual(cleared)
    await expect(listUnavailableRepoUrlsForIntegration(qx, integrationId)).resolves.toEqual([])
  })
})
