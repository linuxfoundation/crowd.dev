import { beforeEach, describe, expect, it, vi } from 'vitest'

import { reconcileGithubIntegration } from './reconcileGithubIntegration'

const calls = vi.hoisted<string[]>(() => [])

const activities = vi.hoisted(() => {
  const names = [
    'discoverFullSyncRepos',
    'planGithubLifecycle',
    'onboardGithubRepos',
    'decommissionChannels',
    'decommissionAllUnits',
    'softDeleteIntegrationRepositories',
    'countUndrainedUnits',
    'deleteDrainedUnits',
    'listUndeletedChannels',
    'logLifecycleSkips',
    'logUndrainedChannels',
    'logGithubLifecycleSummary',
  ] as const
  return Object.fromEntries(names.map((name) => [name, vi.fn()])) as Record<
    (typeof names)[number],
    ReturnType<typeof vi.fn>
  >
})

const mocks = vi.hoisted(() => ({
  sleep: vi.fn(() => Promise.resolve()),
  warn: vi.fn(),
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => activities,
  sleep: mocks.sleep,
  log: { info: vi.fn(), warn: mocks.warn },
}))

const integrationId = 'int-1'
const NEW = 'https://github.com/acme/new'
const REMOVED = 'https://github.com/acme/removed'
const ELSEWHERE = 'https://github.com/acme/elsewhere'

const plan = (overrides: Record<string, unknown> = {}) => ({
  found: true,
  githubNango: true,
  deleted: false,
  desired: 2,
  toCreate: [] as string[],
  toDeleteKeys: [] as string[],
  invalidUrls: [] as string[],
  ownedElsewhere: [] as string[],
  deletesSuppressed: false,
  ...overrides,
})

const lifecycleActivities = [
  'onboardGithubRepos',
  'decommissionChannels',
  'decommissionAllUnits',
  'softDeleteIntegrationRepositories',
  'countUndrainedUnits',
  'deleteDrainedUnits',
  'listUndeletedChannels',
  'logUndrainedChannels',
  'logGithubLifecycleSummary',
] as const

describe('reconcileGithubIntegration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    calls.length = 0
    for (const [name, fn] of Object.entries(activities)) {
      fn.mockImplementation(async () => {
        calls.push(name)
        return undefined
      })
    }
    activities.discoverFullSyncRepos.mockImplementation(async () => {
      calls.push('discoverFullSyncRepos')
      return { discovered: [], skipped: [], renameValidationComplete: true }
    })
    activities.listUndeletedChannels.mockImplementation(async () => {
      calls.push('listUndeletedChannels')
      return []
    })
  })

  const answer = (name: keyof typeof activities, ...values: unknown[]) => {
    const fn = activities[name]
    fn.mockImplementation(async () => {
      calls.push(name)
      return values.length > 1 ? values.shift() : values[0]
    })
  }

  it('creates for never-connected repos, pauses then deletes removed units only after the drain clears, and logs the summary', async () => {
    const discovered = { channelId: 'R_1', channelName: 'https://github.com/Acme/Fresh' }
    activities.discoverFullSyncRepos.mockImplementation(async () => {
      calls.push('discoverFullSyncRepos')
      return {
        discovered: [discovered, { channelId: 'R_2', channelName: 'not-a-url' }],
        skipped: [{ url: 'acme', reason: 'github_error' }],
        renameValidationComplete: true,
      }
    })
    const toCreate = Array.from({ length: 1200 }, (_, i) => `https://github.com/acme/repo-${i}`)
    toCreate[0] = NEW
    toCreate[1100] = 'https://github.com/acme/fresh'
    answer(
      'planGithubLifecycle',
      plan({ toCreate, toDeleteKeys: [REMOVED], ownedElsewhere: [ELSEWHERE] }),
    )
    answer('countUndrainedUnits', 1, 0)
    answer('deleteDrainedUnits', 8)
    activities.onboardGithubRepos.mockImplementation(async ({ keys }: { keys: string[] }) => {
      calls.push('onboardGithubRepos')
      return { created: keys.length - 1, unitsInserted: (keys.length - 1) * 8, skipped: 1 }
    })

    const summary = await reconcileGithubIntegration({ integrationId })

    expect(calls).toEqual([
      'discoverFullSyncRepos',
      'planGithubLifecycle',
      'logLifecycleSkips',
      'decommissionChannels',
      'countUndrainedUnits',
      'countUndrainedUnits',
      'deleteDrainedUnits',
      'listUndeletedChannels',
      'onboardGithubRepos',
      'onboardGithubRepos',
      'onboardGithubRepos',
      'planGithubLifecycle',
      'logGithubLifecycleSummary',
    ])
    expect(activities.decommissionChannels).toHaveBeenCalledWith({
      integrationId,
      channelKeys: [REMOVED],
    })
    expect(activities.onboardGithubRepos.mock.calls.map(([input]) => input)).toEqual([
      { integrationId, keys: toCreate.slice(0, 500), resolved: {}, startIndex: 0 },
      { integrationId, keys: toCreate.slice(500, 1000), resolved: {}, startIndex: 500 },
      {
        integrationId,
        keys: toCreate.slice(1000),
        resolved: { 'https://github.com/acme/fresh': discovered },
        startIndex: 1000,
      },
    ])
    expect(mocks.sleep.mock.calls).toEqual([['30 seconds']])
    expect(summary).toMatchObject({
      integrationId,
      deleted: false,
      desired: 2,
      discovered: 2,
      created: 1197,
      unitsInserted: 9576,
      deletedUnits: 8,
      repositoriesSoftDeleted: 0,
      undrainedChannels: 0,
      deletesSuppressed: false,
      failed: false,
      skipped: 5,
    })
    expect(summary.outcome).toBeUndefined()
    expect(activities.logLifecycleSkips).toHaveBeenCalledWith({
      integrationId,
      skipped: [
        { url: 'acme', reason: 'github_error' },
        { url: ELSEWHERE, reason: 'owned_by_other_integration' },
      ],
    })
    expect(activities.logGithubLifecycleSummary).toHaveBeenCalledWith(summary)

    vi.clearAllMocks()
    answer('discoverFullSyncRepos', { discovered: [], skipped: [], renameValidationComplete: true })
    answer(
      'planGithubLifecycle',
      plan({ toCreate: [NEW], invalidUrls: ['not-a-url'], deletesSuppressed: true }),
    )
    const suppressed = await reconcileGithubIntegration({ integrationId })

    expect(activities.decommissionChannels).not.toHaveBeenCalled()
    expect(activities.countUndrainedUnits).not.toHaveBeenCalled()
    expect(activities.logLifecycleSkips).toHaveBeenCalledWith({
      integrationId,
      skipped: [{ url: 'not-a-url', reason: 'invalid_url' }],
    })
    expect(suppressed).toMatchObject({ deletesSuppressed: true, created: 0, skipped: 2 })
  })

  it('deletes all units and soft-deletes the repositories of a deleted integration and never onboards', async () => {
    answer('planGithubLifecycle', plan({ deleted: true, desired: 0, toDeleteKeys: [REMOVED, NEW] }))
    answer('softDeleteIntegrationRepositories', 3)
    answer('countUndrainedUnits', 0)
    answer('deleteDrainedUnits', 16)

    const summary = await reconcileGithubIntegration({ integrationId })

    expect(calls).toEqual([
      'discoverFullSyncRepos',
      'planGithubLifecycle',
      'decommissionAllUnits',
      'softDeleteIntegrationRepositories',
      'countUndrainedUnits',
      'deleteDrainedUnits',
      'listUndeletedChannels',
      'logGithubLifecycleSummary',
    ])
    expect(activities.decommissionAllUnits).toHaveBeenCalledWith({ integrationId })
    expect(activities.softDeleteIntegrationRepositories).toHaveBeenCalledWith({ integrationId })
    expect(activities.decommissionChannels).not.toHaveBeenCalled()
    expect(activities.onboardGithubRepos).not.toHaveBeenCalled()
    expect(mocks.sleep).not.toHaveBeenCalled()
    expect(summary).toMatchObject({
      deleted: true,
      discovered: 0,
      created: 0,
      deletedUnits: 16,
      repositoriesSoftDeleted: 3,
      undrainedChannels: 0,
      deletesSuppressed: false,
    })

    vi.clearAllMocks()
    calls.length = 0
    const toCreate = Array.from({ length: 1200 }, (_, i) => `https://github.com/acme/repo-${i}`)
    answer('planGithubLifecycle', plan({ toCreate }), plan({ deleted: true, desired: 0 }))
    answer('onboardGithubRepos', {
      created: 10,
      unitsInserted: 80,
      skipped: 490,
      integrationDeleted: true,
    })
    answer('softDeleteIntegrationRepositories', 2)
    answer('countUndrainedUnits', 0)
    answer('deleteDrainedUnits', 80)

    const interrupted = await reconcileGithubIntegration({ integrationId })

    expect(calls).toEqual([
      'discoverFullSyncRepos',
      'planGithubLifecycle',
      'onboardGithubRepos',
      'planGithubLifecycle',
      'decommissionAllUnits',
      'softDeleteIntegrationRepositories',
      'countUndrainedUnits',
      'deleteDrainedUnits',
      'listUndeletedChannels',
      'logGithubLifecycleSummary',
    ])
    expect(interrupted).toMatchObject({
      deleted: true,
      created: 10,
      skipped: 1190,
      deletedUnits: 80,
      repositoriesSoftDeleted: 2,
      failed: false,
    })
  })

  it('runs the delete path when the closing re-plan finds the integration deleted, and warns when onboarding stopped without a deletion', async () => {
    answer('planGithubLifecycle', plan({ toDeleteKeys: [REMOVED] }), plan({ deleted: true }))
    answer('countUndrainedUnits', 0)
    answer('deleteDrainedUnits', 8, 16)
    answer('softDeleteIntegrationRepositories', 4)

    const summary = await reconcileGithubIntegration({ integrationId })

    expect(calls).toEqual([
      'discoverFullSyncRepos',
      'planGithubLifecycle',
      'decommissionChannels',
      'countUndrainedUnits',
      'deleteDrainedUnits',
      'listUndeletedChannels',
      'planGithubLifecycle',
      'decommissionAllUnits',
      'softDeleteIntegrationRepositories',
      'countUndrainedUnits',
      'deleteDrainedUnits',
      'listUndeletedChannels',
      'logGithubLifecycleSummary',
    ])
    expect(summary).toMatchObject({ deleted: true, deletedUnits: 24, repositoriesSoftDeleted: 4 })

    vi.clearAllMocks()
    calls.length = 0
    answer('planGithubLifecycle', plan({ toCreate: [NEW] }), plan({ githubNango: false }))
    answer('onboardGithubRepos', {
      created: 0,
      unitsInserted: 0,
      skipped: 1,
      integrationDeleted: true,
    })

    const stopped = await reconcileGithubIntegration({ integrationId })

    expect(calls).toEqual([
      'discoverFullSyncRepos',
      'planGithubLifecycle',
      'onboardGithubRepos',
      'planGithubLifecycle',
      'logGithubLifecycleSummary',
    ])
    expect(stopped).toMatchObject({ deleted: false, skipped: 1 })
    expect(mocks.warn).toHaveBeenCalledWith(expect.stringContaining('onboarding stopped'), {
      integrationId,
    })
  })

  it('keeps repos discovery skipped as renamed out of onboarding and counts them once', async () => {
    const renamedTo = 'https://github.com/org/new'
    activities.discoverFullSyncRepos.mockImplementation(async () => {
      calls.push('discoverFullSyncRepos')
      return {
        discovered: [],
        skipped: [{ url: 'https://github.com/org/New', reason: 'repo_renamed' }],
        renameValidationComplete: true,
      }
    })
    answer('planGithubLifecycle', plan({ toCreate: [renamedTo, NEW] }))
    answer('onboardGithubRepos', { created: 1, unitsInserted: 8, skipped: 0 })

    const summary = await reconcileGithubIntegration({ integrationId })

    expect(activities.onboardGithubRepos.mock.calls.map(([input]) => input.keys)).toEqual([[NEW]])
    expect(summary).toMatchObject({ created: 1, skipped: 1 })
  })

  it('defers every create while discovery could not validate renames, and still tears down', async () => {
    const a = 'https://github.com/acme/a'
    const b = 'https://github.com/acme/b'
    answer('discoverFullSyncRepos', {
      discovered: [],
      skipped: [
        { url: 'acme', reason: 'incomplete_listing' },
        { url: 'https://github.com/acme/B', reason: 'github_error' },
      ],
      renameValidationComplete: false,
    })
    answer('planGithubLifecycle', plan({ toCreate: [a, b], toDeleteKeys: [REMOVED] }))
    answer('countUndrainedUnits', 0)
    answer('deleteDrainedUnits', 8)

    const summary = await reconcileGithubIntegration({ integrationId })

    expect(activities.onboardGithubRepos).not.toHaveBeenCalled()
    expect(activities.decommissionChannels).toHaveBeenCalledWith({
      integrationId,
      channelKeys: [REMOVED],
    })
    expect(activities.logLifecycleSkips).toHaveBeenCalledWith({
      integrationId,
      skipped: [
        { url: 'acme', reason: 'incomplete_listing' },
        { url: 'https://github.com/acme/B', reason: 'github_error' },
        { url: a, reason: 'incomplete_listing', message: expect.any(String) },
      ],
    })
    expect(summary).toMatchObject({ created: 0, skipped: 3, deletedUnits: 8 })
  })

  it('does nothing but log for a missing or non github-nango integration', async () => {
    answer('planGithubLifecycle', plan({ found: false, githubNango: false }))
    const missing = await reconcileGithubIntegration({ integrationId })

    answer('planGithubLifecycle', plan({ githubNango: false, toCreate: [NEW] }))
    const wrongPlatform = await reconcileGithubIntegration({ integrationId })

    expect(missing.outcome).toBe('not_found')
    expect(wrongPlatform.outcome).toBe('not_github_nango')
    expect(mocks.warn).toHaveBeenCalledTimes(2)
    for (const name of lifecycleActivities) {
      expect(activities[name]).not.toHaveBeenCalled()
    }
    expect(activities.logLifecycleSkips).not.toHaveBeenCalled()
  })

  it('gives up on units still locked after 120 polls, reports them, and logs a failed summary when onboarding or discovery fails', async () => {
    const stuck = 'https://github.com/acme/stuck'
    answer('planGithubLifecycle', plan({ toCreate: [NEW], toDeleteKeys: [stuck, REMOVED] }))
    answer('countUndrainedUnits', 1)
    answer('deleteDrainedUnits', 8)
    answer('listUndeletedChannels', [stuck])
    activities.onboardGithubRepos.mockRejectedValue(new Error('onboard timed out'))

    await expect(reconcileGithubIntegration({ integrationId })).rejects.toThrow('onboard timed out')

    expect(activities.countUndrainedUnits).toHaveBeenCalledTimes(120)
    expect(mocks.sleep).toHaveBeenCalledTimes(120)
    expect(activities.logUndrainedChannels).toHaveBeenCalledWith({
      integrationId,
      channelKeys: [stuck],
    })
    expect(calls.indexOf('logUndrainedChannels')).toBeLessThan(
      calls.indexOf('logGithubLifecycleSummary'),
    )
    expect(activities.logGithubLifecycleSummary).toHaveBeenCalledTimes(1)
    expect(activities.logGithubLifecycleSummary).toHaveBeenCalledWith(
      expect.objectContaining({ failed: true, deletedUnits: 8, undrainedChannels: 1, created: 0 }),
    )

    vi.clearAllMocks()
    activities.discoverFullSyncRepos.mockRejectedValue(new Error('discovery timed out'))
    await expect(reconcileGithubIntegration({ integrationId })).rejects.toThrow(
      'discovery timed out',
    )
    expect(activities.planGithubLifecycle).not.toHaveBeenCalled()
    expect(activities.logGithubLifecycleSummary).toHaveBeenCalledWith(
      expect.objectContaining({ failed: true, desired: 0, discovered: 0, created: 0, skipped: 0 }),
    )
  })
})
