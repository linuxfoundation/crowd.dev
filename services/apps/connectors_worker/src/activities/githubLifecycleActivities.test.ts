import axios from 'axios'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  discoverFullSyncRepos,
  onboardGithubRepos,
  planGithubLifecycle,
} from './githubLifecycleActivities'

const mocks = vi.hoisted(() => ({
  fetchGithubLifecycleIntegration: vi.fn(),
  getNangoMappingsForIntegrations: vi.fn(),
  addRepoToGitIntegration: vi.fn(),
  listGithubChannelsOwnedElsewhere: vi.fn(),
  listGithubIntegrationsNeedingReconcile: vi.fn(),
  listGithubRepoKeysUnderOwners: vi.fn(),
  getReposForGithubIntegration: vi.fn(),
  upsertRepository: vi.fn(),
  softDeleteRepositoriesForIntegration: vi.fn(),
  findRepositoryOwnersForSegment: vi.fn(),
  findRepositoryByUrlInsensitive: vi.fn(),
  listSyncUnitChannelsForIntegration: vi.fn(),
  onboardGithubRepo: vi.fn(),
  getCredential: vi.fn(),
  resolveInstallationId: vi.fn(),
  mintInstallationToken: vi.fn(),
  listOrgRepos: vi.fn(),
  resolveRepo: vi.fn(),
  heartbeat: vi.fn(),
  sleep: vi.fn(),
  qx: { tx: vi.fn() },
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('../main', () => ({ svc: { postgres: { writer: {} }, log: mocks.log } }))
vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({ dbStoreQx: vi.fn(() => mocks.qx) }))
vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  listSyncUnitChannelsForIntegration: mocks.listSyncUnitChannelsForIntegration,
}))
vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  fetchGithubLifecycleIntegration: mocks.fetchGithubLifecycleIntegration,
  getNangoMappingsForIntegrations: mocks.getNangoMappingsForIntegrations,
  addRepoToGitIntegration: mocks.addRepoToGitIntegration,
  listGithubChannelsOwnedElsewhere: mocks.listGithubChannelsOwnedElsewhere,
  listGithubIntegrationsNeedingReconcile: mocks.listGithubIntegrationsNeedingReconcile,
  listGithubRepoKeysUnderOwners: mocks.listGithubRepoKeysUnderOwners,
}))
vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  getReposForGithubIntegration: mocks.getReposForGithubIntegration,
  upsertRepository: mocks.upsertRepository,
  softDeleteRepositoriesForIntegration: mocks.softDeleteRepositoriesForIntegration,
  findRepositoryOwnersForSegment: mocks.findRepositoryOwnersForSegment,
  findRepositoryByUrlInsensitive: mocks.findRepositoryByUrlInsensitive,
}))
vi.mock('../onboardGithubRepo', () => ({ onboardGithubRepo: mocks.onboardGithubRepo }))
vi.mock('@crowd/connectors', () => ({ getCredential: mocks.getCredential }))
vi.mock('@crowd/connectors/src/connectors/github/appToken', () => ({
  resolveInstallationId: mocks.resolveInstallationId,
  mintInstallationToken: mocks.mintInstallationToken,
}))
vi.mock('@crowd/connectors/src/connectors/github/discover', () => ({
  listOrgRepos: mocks.listOrgRepos,
  resolveRepo: mocks.resolveRepo,
}))
vi.mock('@temporalio/activity', () => ({
  Context: { current: () => ({ heartbeat: mocks.heartbeat, sleep: mocks.sleep }) },
}))

const NOW = new Date('2026-10-22T10:00:00Z')

const orgRepo = (name: string, fork = false) => ({
  channelId: `id-${name}`,
  channelName: `https://github.com/Acme/${name}`,
  fork,
})

const channelOf = ({ channelId, channelName }: { channelId: string; channelName: string }) => ({
  channelId,
  channelName,
})

const owners = { gitIntegrationId: 'git-1', insightsProjectId: 'ip-1' }

const nangoIntegration = (orgs: { name: string; fullSync: boolean }[]) => ({
  id: 'int-1',
  platform: 'github-nango',
  segmentId: 'seg-1',
  settings: { orgs },
  deletedAt: null,
})

const httpError = (code: number) =>
  new axios.AxiosError(String(code), String(code), undefined, undefined, {
    status: code,
    headers: {},
  } as never)

const rateLimitError = () =>
  new axios.AxiosError('Too Many Requests', '429', undefined, undefined, {
    status: 429,
    headers: {},
  } as never)

describe('github lifecycle activities', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ now: NOW, toFake: ['Date'] })
    mocks.qx.tx.mockImplementation(async (fn: (inner: typeof mocks.qx) => Promise<unknown>) =>
      fn(mocks.qx),
    )
    mocks.sleep.mockImplementation(async (ms: number) => {
      vi.setSystemTime(Date.now() + ms)
    })
    mocks.getCredential.mockResolvedValue({ platform: 'github-nango' })
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration([{ name: 'acme', fullSync: true }]),
    )
    mocks.getNangoMappingsForIntegrations.mockResolvedValue({})
    mocks.listGithubRepoKeysUnderOwners.mockResolvedValue([])
    mocks.resolveInstallationId.mockResolvedValue('inst-1')
    mocks.mintInstallationToken.mockImplementation(async () => ({
      token: 'token',
      expiresAt: new Date(Date.now() + 6 * 60_000).toISOString(),
    }))
  })
  afterEach(() => vi.useRealTimers())

  it('discovers fullSync repos sequentially, writes the repository row and git remote, and skips owned-elsewhere/segment_not_ready/invalid org/list errors with reasons', async () => {
    const known = orgRepo('Known')
    const fresh = orgRepo('Fresh')
    const elsewhere = orgRepo('Elsewhere')
    const fork = orgRepo('Fork', true)
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration([
        { name: 'acme', fullSync: true },
        { name: 'bad name!', fullSync: true },
        { name: 'partial', fullSync: false },
      ]),
    )
    mocks.getReposForGithubIntegration.mockResolvedValue([{ url: known.channelName }])
    mocks.findRepositoryOwnersForSegment.mockResolvedValue(owners)
    mocks.listOrgRepos.mockImplementation(async (_token: string, org: string) => {
      if (org === 'broken') {
        throw new Error('boom')
      }
      return [known, fresh, elsewhere, fork]
    })
    mocks.resolveRepo.mockResolvedValue({
      ...channelOf(fork),
      forkedFrom: 'https://github.com/upstream/Fork',
    })

    const dbEvents: string[] = []
    let dbInFlight = 0
    const dbWrite =
      <T>(label: string, value: (url: string) => T) =>
      async (_qx: unknown, ...args: unknown[]) => {
        const url = typeof args[0] === 'string' ? args[0] : (args[0] as { url: string }).url
        const target = typeof args[1] === 'string' ? args[1] : url
        if (dbInFlight > 0) {
          throw new Error(`overlapping db write ${label}`)
        }
        dbInFlight++
        await Promise.resolve()
        dbEvents.push(`${label}:${target.split('/').pop()}`)
        dbInFlight--
        return value(target)
      }
    mocks.findRepositoryByUrlInsensitive.mockImplementation(
      dbWrite('find', (url) =>
        url === elsewhere.channelName ? { url, sourceIntegrationId: 'int-other' } : null,
      ),
    )
    mocks.upsertRepository.mockImplementation(
      dbWrite('upsert', (url) => (url === fresh.channelName ? 'inserted' : 'restored')),
    )
    mocks.addRepoToGitIntegration.mockImplementation(dbWrite('remote', () => undefined))

    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [channelOf(fresh), channelOf(fork)],
      skipped: [
        { url: 'bad name!', reason: 'invalid_url', message: expect.any(String) },
        {
          url: elsewhere.channelName,
          reason: 'owned_by_other_integration',
          message: expect.any(String),
        },
      ],
    })

    expect(dbEvents).toEqual([
      'find:Fresh',
      'upsert:Fresh',
      'remote:Fresh',
      'find:Elsewhere',
      'find:Fork',
      'upsert:Fork',
      'remote:Fork',
    ])
    expect(mocks.mintInstallationToken).toHaveBeenCalledTimes(1)
    expect(mocks.listOrgRepos).toHaveBeenCalledWith('token', 'acme', expect.any(Function))
    const heartbeats = mocks.heartbeat.mock.calls.length
    mocks.listOrgRepos.mock.calls[0][2]()
    expect(mocks.heartbeat).toHaveBeenCalledTimes(heartbeats + 1)
    expect(mocks.listOrgRepos).not.toHaveBeenCalledWith('token', 'bad name!', expect.any(Function))
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(1)
    expect(mocks.resolveRepo).toHaveBeenCalledWith('token', 'Acme', 'Fork')
    expect(mocks.upsertRepository).toHaveBeenCalledWith(mocks.qx, {
      id: expect.any(String),
      url: fork.channelName,
      segmentId: 'seg-1',
      gitIntegrationId: 'git-1',
      sourceIntegrationId: 'int-1',
      insightsProjectId: 'ip-1',
      forkedFrom: 'https://github.com/upstream/Fork',
    })
    expect(mocks.addRepoToGitIntegration).toHaveBeenCalledWith(mocks.qx, 'int-1', fresh.channelName)
    expect(mocks.heartbeat).toHaveBeenCalled()

    vi.clearAllMocks()
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration([
        { name: 'acme', fullSync: true },
        { name: 'broken', fullSync: true },
        { name: 'other', fullSync: true },
      ]),
    )
    mocks.listOrgRepos.mockRejectedValue(rateLimitError())
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: ['acme', 'broken', 'other'].map((url) =>
        expect.objectContaining({ url, reason: 'rate_limited' }),
      ),
    })
    expect(mocks.listOrgRepos).toHaveBeenCalledTimes(1)

    vi.clearAllMocks()
    mocks.findRepositoryOwnersForSegment.mockResolvedValue(null)
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: ['acme', 'broken', 'other'].map((url) => ({
        url,
        reason: 'segment_not_ready',
        message: expect.any(String),
      })),
    })

    mocks.fetchGithubLifecycleIntegration.mockResolvedValue({
      ...nangoIntegration([{ name: 'acme', fullSync: true }]),
      deletedAt: '2026-10-01T00:00:00Z',
    })
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [],
      skipped: [],
    })
    expect(mocks.mintInstallationToken).not.toHaveBeenCalled()
    expect(mocks.listOrgRepos).not.toHaveBeenCalled()
    expect(mocks.upsertRepository).not.toHaveBeenCalled()

    vi.clearAllMocks()
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration([
        { name: 'acme', fullSync: true },
        { name: 'broken', fullSync: true },
      ]),
    )
    mocks.findRepositoryOwnersForSegment.mockResolvedValue(owners)
    mocks.listOrgRepos.mockImplementation(async (_token: string, org: string) => {
      if (org === 'broken') {
        throw httpError(502)
      }
      return [fresh]
    })
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: [
        { url: 'broken', reason: 'github_error', message: '502' },
        { url: 'acme', reason: 'incomplete_listing', message: 'listing failed for broken' },
      ],
    })
    expect(mocks.upsertRepository).not.toHaveBeenCalled()
    expect(mocks.listGithubRepoKeysUnderOwners).not.toHaveBeenCalled()

    vi.clearAllMocks()
    const listingStatus: Record<string, number> = { broken: 404, blocked: 451, forbidden: 403 }
    mocks.listOrgRepos.mockImplementation(async (_token: string, org: string) => {
      if (org in listingStatus) {
        throw httpError(listingStatus[org])
      }
      return [fresh]
    })
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration(['acme', 'broken', 'blocked'].map((name) => ({ name, fullSync: true }))),
    )
    mocks.getReposForGithubIntegration.mockResolvedValue([
      { url: known.channelName },
      { url: 'https://github.com/broken/x' },
    ])
    mocks.resolveRepo.mockRejectedValue(httpError(404))
    mocks.findRepositoryByUrlInsensitive.mockResolvedValue(null)
    mocks.upsertRepository.mockResolvedValue('inserted')
    mocks.addRepoToGitIntegration.mockResolvedValue(undefined)
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [channelOf(fresh)],
      skipped: ['broken', 'blocked'].map((url) => ({
        url,
        reason: 'repo_not_found',
        message: 'org not found on GitHub',
      })),
    })
    expect(mocks.listGithubRepoKeysUnderOwners).toHaveBeenCalledWith(mocks.qx, ['acme'])
    expect(mocks.resolveRepo).toHaveBeenCalledWith('token', 'broken', 'x')

    vi.clearAllMocks()
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration(['acme', 'forbidden'].map((name) => ({ name, fullSync: true }))),
    )
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: [
        { url: 'forbidden', reason: 'github_error', message: '403' },
        { url: 'acme', reason: 'incomplete_listing', message: 'listing failed for forbidden' },
      ],
    })
    expect(mocks.upsertRepository).not.toHaveBeenCalled()

    vi.clearAllMocks()
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration([{ name: 'broken', fullSync: true }]),
    )
    mocks.getReposForGithubIntegration.mockResolvedValue([{ url: 'https://github.com/alice/x' }])
    mocks.getNangoMappingsForIntegrations.mockResolvedValue({
      'int-1': { 'conn-1': { owner: 'broken', repoName: 'widget', repositoryId: null } },
    })
    mocks.resolveRepo
      .mockResolvedValueOnce({
        channelId: 'R_Y',
        channelName: 'https://github.com/alice/y',
        forkedFrom: null,
      })
      .mockResolvedValueOnce({
        channelId: 'R_W',
        channelName: 'https://github.com/NewOrg/widget',
        forkedFrom: null,
      })
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [],
      skipped: [
        { url: 'broken', reason: 'repo_not_found', message: 'org not found on GitHub' },
        {
          url: 'https://github.com/alice/y',
          reason: 'repo_renamed',
          message: 'renamed from https://github.com/alice/x',
        },
        {
          url: 'https://github.com/neworg/widget',
          reason: 'repo_renamed',
          message: 'renamed from https://github.com/broken/widget',
        },
      ],
    })
    expect(mocks.resolveRepo.mock.calls.map(([, owner, name]) => `${owner}/${name}`)).toEqual([
      'alice/x',
      'broken/widget',
    ])

    vi.clearAllMocks()
    mocks.getReposForGithubIntegration.mockResolvedValue([])
    mocks.resolveRepo.mockRejectedValueOnce(rateLimitError())
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: [
        { url: 'broken', reason: 'repo_not_found', message: 'org not found on GitHub' },
        {
          url: 'https://github.com/broken/widget',
          reason: 'rate_limited',
          message: 'Too Many Requests',
        },
      ],
    })

    vi.clearAllMocks()
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [],
      skipped: [{ url: 'broken', reason: 'repo_not_found', message: 'org not found on GitHub' }],
    })
    expect(mocks.resolveRepo).toHaveBeenCalledWith('token', 'broken', 'widget')
    mocks.getNangoMappingsForIntegrations.mockResolvedValue({})

    vi.clearAllMocks()
    const forks = ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7'].map((name) => orgRepo(name, true))
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(
      nangoIntegration([{ name: 'acme', fullSync: true }]),
    )
    mocks.getReposForGithubIntegration.mockResolvedValue([])
    mocks.listOrgRepos.mockResolvedValue(forks)
    mocks.findRepositoryByUrlInsensitive.mockResolvedValue(null)
    mocks.upsertRepository.mockResolvedValue('inserted')
    mocks.addRepoToGitIntegration.mockResolvedValue(undefined)
    mocks.resolveRepo.mockImplementation(async (_token: string, _owner: string, name: string) => {
      if (name === 'F2') {
        throw rateLimitError()
      }
      return { ...channelOf(orgRepo(name, true)), forkedFrom: null }
    })
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [forks[0], forks[2], forks[3], forks[4]].map(channelOf),
      skipped: [
        { url: forks[1].channelName, reason: 'rate_limited', message: 'Too Many Requests' },
        ...[forks[5], forks[6]].map((repo) => ({
          url: repo.channelName,
          reason: 'rate_limited',
          message: 'rate limited while resolving forks of acme',
        })),
      ],
    })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(5)

    vi.clearAllMocks()
    mocks.listOrgRepos.mockResolvedValue([fresh])
    mocks.addRepoToGitIntegration.mockRejectedValueOnce(new Error('git remote write failed'))
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).rejects.toThrow(
      'git remote write failed',
    )
    expect(mocks.qx.tx).toHaveBeenCalledTimes(1)
    expect(mocks.upsertRepository).toHaveBeenCalledWith(
      mocks.qx,
      expect.objectContaining({ url: fresh.channelName }),
    )
  })

  it('detects renames across orgs, integrations and org renames in one pass, isolates per-key errors, and stops on deletion, rate limit or a failed mint', async () => {
    const repoOf = (org: string, name: string) => ({
      channelId: `id-${org}-${name}`,
      channelName: `https://github.com/${org}/${name}`,
      fork: false,
    })
    const key = (org: string, name: string) => `https://github.com/${org}/${name}`
    const listings: Record<string, ReturnType<typeof repoOf>[]> = {
      acme: [repoOf('acme', 'new'), repoOf('acme', 'fresh')],
      beta: [repoOf('beta', 'x'), repoOf('beta', 'kept')],
      neworg: [repoOf('neworg', 'bbb'), repoOf('neworg', 'ccc'), repoOf('neworg', 'c')],
    }
    const renames: Record<string, string | number> = {
      'acme/x': key('beta', 'x'),
      'acme/old': key('acme', 'new'),
      'acme/blocked': 451,
      'acme/gone': 404,
      'oldorg/aaa': 404,
      'oldorg/bbb': key('neworg', 'bbb'),
      'oldorg/ccc': key('neworg', 'ccc'),
    }
    const orgs = ['acme', 'beta', 'neworg'].map((name) => ({ name, fullSync: true }))
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(nangoIntegration(orgs))
    mocks.getReposForGithubIntegration.mockResolvedValue(
      ['aaa', 'bbb', 'ccc'].map((name) => ({ url: key('oldorg', name) })),
    )
    mocks.getNangoMappingsForIntegrations.mockResolvedValue({
      'int-1': { 'conn-1': { owner: 'acme', repoName: 'x', repositoryId: null } },
    })
    mocks.listGithubRepoKeysUnderOwners.mockResolvedValue([
      key('acme', 'x'),
      key('acme', 'old'),
      key('acme', 'blocked'),
      key('acme', 'gone'),
      key('beta', 'kept'),
      'https://github.com/acme/x/tree/main',
      'https://github.com/acme/y.git',
    ])
    mocks.findRepositoryOwnersForSegment.mockResolvedValue(owners)
    mocks.findRepositoryByUrlInsensitive.mockResolvedValue(null)
    mocks.upsertRepository.mockResolvedValue('inserted')
    mocks.listOrgRepos.mockImplementation(async (_token: string, org: string) => listings[org])
    mocks.resolveRepo.mockImplementation(async (_token: string, owner: string, name: string) => {
      const target = renames[`${owner}/${name}`]
      if (typeof target === 'number') {
        throw httpError(target)
      }
      return { channelId: `id-${target}`, channelName: target, forkedFrom: null }
    })

    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [
        channelOf(listings.acme[1]),
        channelOf(listings.beta[1]),
        channelOf(listings.neworg[2]),
      ],
      skipped: [
        { url: key('acme', 'blocked'), reason: 'github_error', message: '451' },
        {
          url: key('acme', 'new'),
          reason: 'repo_renamed',
          message: `${key('acme', 'new')} is the new name of ${key('acme', 'old')}, a repository tracked elsewhere in CDP`,
        },
        ...[key('beta', 'x'), key('neworg', 'bbb'), key('neworg', 'ccc')].map((url) => ({
          url,
          reason: 'repo_renamed',
          message: expect.stringContaining('which this integration already tracks'),
        })),
      ],
    })
    expect(mocks.listGithubRepoKeysUnderOwners).toHaveBeenCalledTimes(1)
    expect(mocks.listGithubRepoKeysUnderOwners).toHaveBeenCalledWith(mocks.qx, [
      'acme',
      'beta',
      'neworg',
    ])
    expect(
      mocks.resolveRepo.mock.calls.map(([, owner, name]) => `${owner}/${name}`).sort(),
    ).toEqual([
      'acme/blocked',
      'acme/gone',
      'acme/old',
      'acme/x',
      'oldorg/aaa',
      'oldorg/bbb',
      'oldorg/ccc',
    ])
    expect(mocks.upsertRepository.mock.calls.map(([, row]) => row.url)).toEqual([
      key('acme', 'fresh'),
      key('beta', 'kept'),
      key('neworg', 'c'),
    ])

    vi.clearAllMocks()
    mocks.getReposForGithubIntegration.mockResolvedValue(
      Array.from({ length: 12 }, (_, i) => ({ url: key('oldorg', `a${i + 1}`) })),
    )
    mocks.getNangoMappingsForIntegrations.mockResolvedValue({})
    mocks.listGithubRepoKeysUnderOwners.mockResolvedValue([])
    mocks.resolveRepo.mockRejectedValue(httpError(404))
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [
        channelOf(listings.acme[0]),
        channelOf(listings.acme[1]),
        ...listings.beta.map(channelOf),
        ...listings.neworg.map(channelOf),
      ],
      skipped: [
        {
          url: 'oldorg',
          reason: 'repo_not_found',
          message: 'none of the probed repositories exist on GitHub',
        },
      ],
    })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(10)
    expect(mocks.upsertRepository).toHaveBeenCalledTimes(7)

    vi.clearAllMocks()
    mocks.getReposForGithubIntegration.mockResolvedValueOnce(
      ['a', 'b', 'c', 'd', 'e'].map((name) => ({ url: key('oldorg', name) })),
    )
    mocks.listOrgRepos.mockImplementationOnce(async () => [])
    mocks.listOrgRepos.mockImplementationOnce(async () => [])
    mocks.listOrgRepos.mockImplementationOnce(async () =>
      ['d', 'e', 'f'].map((name) => repoOf('neworg', name)),
    )
    mocks.resolveRepo
      .mockRejectedValueOnce(httpError(404))
      .mockRejectedValueOnce(httpError(404))
      .mockRejectedValueOnce(httpError(404))
      .mockResolvedValueOnce({ ...channelOf(repoOf('neworg', 'd')), forkedFrom: null })
      .mockResolvedValueOnce({ ...channelOf(repoOf('neworg', 'e')), forkedFrom: null })
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [channelOf(repoOf('neworg', 'f'))],
      skipped: [key('neworg', 'd'), key('neworg', 'e')].map((url) => ({
        url,
        reason: 'repo_renamed',
        message: expect.stringContaining('which this integration already tracks'),
      })),
    })
    expect(mocks.resolveRepo.mock.calls.map(([, owner, name]) => `${owner}/${name}`)).toEqual([
      'oldorg/a',
      'oldorg/b',
      'oldorg/c',
      'oldorg/d',
      'oldorg/e',
    ])

    vi.clearAllMocks()
    mocks.resolveRepo.mockRejectedValueOnce(httpError(404)).mockRejectedValueOnce(httpError(502))
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: ['acme', 'beta', 'neworg'].map((url) => ({
        url,
        reason: 'incomplete_listing',
        message: `could not resolve ${key('oldorg', 'a2')}: 502`,
      })),
    })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(2)
    expect(mocks.upsertRepository).not.toHaveBeenCalled()

    vi.clearAllMocks()
    mocks.getReposForGithubIntegration.mockResolvedValue([])
    mocks.listGithubRepoKeysUnderOwners.mockResolvedValue([key('acme', 'old')])
    mocks.resolveRepo.mockRejectedValue(rateLimitError())
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: ['acme', 'beta', 'neworg'].map((url) =>
        expect.objectContaining({ url, reason: 'rate_limited' }),
      ),
    })
    expect(mocks.upsertRepository).not.toHaveBeenCalled()

    vi.clearAllMocks()
    mocks.resolveRepo.mockRejectedValue(httpError(502))
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: ['acme', 'beta', 'neworg'].map((url) => ({
        url,
        reason: 'incomplete_listing',
        message: `could not resolve ${key('acme', 'old')}: 502`,
      })),
    })
    expect(mocks.upsertRepository).not.toHaveBeenCalled()

    vi.clearAllMocks()
    const expired = { token: 'token', expiresAt: NOW.toISOString() }
    mocks.mintInstallationToken
      .mockResolvedValueOnce(expired)
      .mockResolvedValueOnce(expired)
      .mockResolvedValueOnce(expired)
      .mockRejectedValueOnce(new Error('mint failed'))
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: ['acme', 'beta', 'neworg'].map((url) => ({
        url,
        reason: 'token_mint_failed',
        message: 'mint failed',
      })),
    })
    expect(mocks.resolveRepo).not.toHaveBeenCalled()

    vi.clearAllMocks()
    const twoOrgs = [
      { name: 'acme', fullSync: true },
      { name: 'beta', fullSync: true },
    ]
    mocks.listGithubRepoKeysUnderOwners.mockResolvedValue([])
    mocks.fetchGithubLifecycleIntegration
      .mockResolvedValueOnce(nangoIntegration(twoOrgs))
      .mockResolvedValue({ ...nangoIntegration(twoOrgs), deletedAt: '2026-10-22T09:59:00Z' })
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: [],
      skipped: ['acme', 'beta'].map((url) => ({
        url,
        reason: 'integration_deleted',
        message: expect.any(String),
      })),
    })
    expect(mocks.upsertRepository).not.toHaveBeenCalled()

    vi.clearAllMocks()
    mocks.fetchGithubLifecycleIntegration
      .mockResolvedValueOnce(nangoIntegration(twoOrgs))
      .mockResolvedValue(
        nangoIntegration([
          { name: 'acme', fullSync: false },
          { name: 'beta', fullSync: true },
        ]),
      )
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: true,
      discovered: listings.beta.map(channelOf),
      skipped: [{ url: 'acme', reason: 'org_not_full_sync', message: expect.any(String) }],
    })

    vi.clearAllMocks()
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(nangoIntegration(twoOrgs))
    mocks.listOrgRepos.mockResolvedValue([])
    mocks.resolveInstallationId.mockRejectedValueOnce(new Error('no installation'))
    await expect(discoverFullSyncRepos({ integrationId: 'int-1' })).resolves.toEqual({
      renameValidationComplete: false,
      discovered: [],
      skipped: [
        { url: 'acme', reason: 'token_mint_failed', message: 'no installation' },
        { url: 'beta', reason: 'incomplete_listing', message: 'listing failed for acme' },
      ],
    })
    expect(mocks.getCredential).toHaveBeenCalledTimes(2)
    expect(mocks.listOrgRepos).toHaveBeenCalledTimes(1)
    expect(mocks.listOrgRepos).toHaveBeenCalledWith('token', 'beta', expect.any(Function))
  })

  it('onboards keys in chunks of five, skips the REST resolve for pre-resolved channels, spreads initial delays, waits until resetAt once on a rate limit and logs each skip', async () => {
    const keys = ['r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7'].map(
      (r) => `https://github.com/acme/${r}`,
    )
    const resolved = {
      [keys[0]]: { channelId: 'id-r1', channelName: keys[0] },
      [keys[5]]: { channelId: 'id-r6', channelName: keys[5] },
    }
    const resetOnboardScenario = (desiredUrls: string[]) => {
      vi.clearAllMocks()
      mocks.onboardGithubRepo.mockReset()
      mocks.fetchGithubLifecycleIntegration.mockReset().mockResolvedValue(nangoIntegration([]))
      mocks.getReposForGithubIntegration
        .mockReset()
        .mockResolvedValue(desiredUrls.map((url) => ({ url })))
    }
    resetOnboardScenario(['https://github.com/acme/R1', ...keys.slice(1)])
    let inFlight = 0
    let peak = 0
    const attempts = new Map<string, number>()
    mocks.onboardGithubRepo.mockImplementation(
      async (_qx: unknown, _token: string, _id: string, key: string) => {
        inFlight++
        peak = Math.max(peak, inFlight)
        await Promise.resolve()
        inFlight--
        const attempt = (attempts.get(key) ?? 0) + 1
        attempts.set(key, attempt)
        if (key === keys[1]) {
          return { outcome: 'skipped', reason: 'repo_not_found', message: '404' }
        }
        if (key === keys[2] && attempt === 1) {
          return {
            outcome: 'skipped',
            reason: 'rate_limited',
            message: '429',
            resetAt: '2026-10-22T10:02:30.000Z',
          }
        }
        if (key === keys[6]) {
          return { outcome: 'skipped', reason: 'already_present', message: 'units exist' }
        }
        return { outcome: 'created', unitsInserted: 8 }
      },
    )

    await expect(onboardGithubRepos({ integrationId: 'int-1', keys, resolved })).resolves.toEqual({
      created: 5,
      unitsInserted: 40,
      skipped: 2,
      integrationDeleted: false,
    })

    expect(peak).toBe(5)
    keys.forEach((key, i) =>
      expect(mocks.onboardGithubRepo).toHaveBeenCalledWith(mocks.qx, 'token', 'int-1', key, {
        storedUrl: i === 0 ? 'https://github.com/acme/R1' : key,
        initialDelaySeconds: i * 2,
        resolved: resolved[key],
      }),
    )
    expect(attempts.get(keys[2])).toBe(2)
    expect(mocks.onboardGithubRepo).toHaveBeenCalledTimes(8)
    expect(mocks.sleep.mock.calls.map(([ms]) => ms)).toEqual([...Array(7).fill(20_000), 10_000])
    expect(mocks.mintInstallationToken).toHaveBeenCalledTimes(2)
    expect(mocks.heartbeat).toHaveBeenCalled()
    expect(mocks.log.warn).toHaveBeenCalledTimes(2)
    expect(mocks.log.warn).toHaveBeenCalledWith(
      {
        event: 'lifecycle_repo_skipped',
        integrationId: 'int-1',
        url: keys[1],
        reason: 'repo_not_found',
        message: '404',
      },
      'repo skipped by github lifecycle',
    )

    resetOnboardScenario(keys)
    mocks.onboardGithubRepo.mockImplementation(
      async (_qx: unknown, _token: string, _id: string, key: string) =>
        key === keys[0]
          ? {
              outcome: 'skipped',
              reason: 'rate_limited',
              resetAt: new Date(Date.now() + 60_000).toISOString(),
            }
          : { outcome: 'created', unitsInserted: 8 },
    )

    await expect(
      onboardGithubRepos({ integrationId: 'int-1', keys, resolved: {} }),
    ).resolves.toEqual({ created: 4, unitsInserted: 32, skipped: 3, integrationDeleted: false })
    expect(mocks.onboardGithubRepo).toHaveBeenCalledTimes(6)
    expect(mocks.sleep.mock.calls.map(([ms]) => ms)).toEqual([20_000, 20_000, 20_000])
    expect(mocks.log.warn).toHaveBeenCalledTimes(3)
    expect(mocks.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ url: keys[6], reason: 'rate_limited' }),
      'repo skipped by github lifecycle',
    )

    resetOnboardScenario(keys)
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue({
      ...nangoIntegration([]),
      deletedAt: '2026-10-22T09:59:00Z',
    })
    await expect(
      onboardGithubRepos({ integrationId: 'int-1', keys: keys.slice(0, 2), resolved: {} }),
    ).resolves.toEqual({ created: 0, unitsInserted: 0, skipped: 2, integrationDeleted: true })
    expect(mocks.onboardGithubRepo).not.toHaveBeenCalled()
    expect(mocks.mintInstallationToken).not.toHaveBeenCalled()
    expect(mocks.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ url: keys[0], reason: 'integration_deleted' }),
      'repo skipped by github lifecycle',
    )

    resetOnboardScenario([keys[0], keys[2]])
    mocks.onboardGithubRepo.mockResolvedValue({ outcome: 'created', unitsInserted: 8 })
    await expect(
      onboardGithubRepos({
        integrationId: 'int-1',
        keys: keys.slice(0, 3),
        resolved: {},
        startIndex: 1000,
      }),
    ).resolves.toEqual({ created: 2, unitsInserted: 16, skipped: 1, integrationDeleted: false })
    expect(
      mocks.onboardGithubRepo.mock.calls.map(([, , , key, options]) => [
        key,
        options.initialDelaySeconds,
      ]),
    ).toEqual([
      [keys[0], 2000],
      [keys[2], 2002],
    ])
    expect(mocks.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ url: keys[1], reason: 'no_longer_desired' }),
      'repo skipped by github lifecycle',
    )

    const many = Array.from({ length: 125 }, (_, i) => `https://github.com/acme/m${i}`)
    resetOnboardScenario(many)
    mocks.onboardGithubRepo.mockResolvedValue({ outcome: 'created', unitsInserted: 8 })
    mocks.fetchGithubLifecycleIntegration
      .mockResolvedValueOnce(nangoIntegration([]))
      .mockResolvedValueOnce({ ...nangoIntegration([]), deletedAt: '2026-10-22T10:30:00Z' })
    await expect(
      onboardGithubRepos({ integrationId: 'int-1', keys: many, resolved: {} }),
    ).resolves.toEqual({ created: 100, unitsInserted: 800, skipped: 25, integrationDeleted: true })
    expect(mocks.onboardGithubRepo).toHaveBeenCalledTimes(100)
    expect(mocks.fetchGithubLifecycleIntegration).toHaveBeenCalledTimes(2)
    expect(mocks.log.warn).toHaveBeenCalledTimes(25)
    expect(mocks.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ url: many[100], reason: 'integration_deleted' }),
      'repo skipped by github lifecycle',
    )

    resetOnboardScenario(many)
    mocks.onboardGithubRepo.mockResolvedValue({ outcome: 'created', unitsInserted: 8 })
    mocks.getReposForGithubIntegration
      .mockResolvedValueOnce(many.map((url) => ({ url })))
      .mockRejectedValueOnce(new Error('db down'))
    await expect(
      onboardGithubRepos({ integrationId: 'int-1', keys: many, resolved: {} }),
    ).resolves.toEqual({ created: 125, unitsInserted: 1000, skipped: 0, integrationDeleted: false })
    expect(mocks.onboardGithubRepo).toHaveBeenCalledTimes(125)
    expect(mocks.getReposForGithubIntegration).toHaveBeenCalledTimes(2)
    expect(mocks.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({
        event: 'lifecycle_onboard_recheck_failed',
        integrationId: 'int-1',
      }),
      expect.any(String),
    )

    resetOnboardScenario(keys)
    mocks.getReposForGithubIntegration
      .mockResolvedValueOnce(keys.map((url) => ({ url })))
      .mockResolvedValueOnce(
        keys.filter((key) => key !== keys[1] && key !== keys[5]).map((url) => ({ url })),
      )
    mocks.onboardGithubRepo.mockImplementation(
      async (_qx: unknown, _token: string, _id: string, key: string) =>
        key === keys[1]
          ? {
              outcome: 'skipped',
              reason: 'rate_limited',
              resetAt: new Date(Date.now() + 60_000).toISOString(),
            }
          : { outcome: 'created', unitsInserted: 8 },
    )
    await expect(
      onboardGithubRepos({ integrationId: 'int-1', keys, resolved: {} }),
    ).resolves.toEqual({ created: 5, unitsInserted: 40, skipped: 2, integrationDeleted: false })
    expect(mocks.onboardGithubRepo.mock.calls.map(([, , , key]) => key)).toEqual([
      ...keys.slice(0, 5),
      keys[6],
    ])
    expect(
      mocks.onboardGithubRepo.mock.calls.filter(([, , , key]) => key === keys[1]),
    ).toHaveLength(1)
    expect(mocks.sleep).toHaveBeenCalled()
    expect(mocks.getReposForGithubIntegration).toHaveBeenCalledTimes(2)
    for (const url of [keys[1], keys[5]]) {
      expect(mocks.log.warn).toHaveBeenCalledWith(
        expect.objectContaining({ url, reason: 'no_longer_desired' }),
        'repo skipped by github lifecycle',
      )
    }
  })

  it('plans the lifecycle from live repos, lower-cased mapping keys, channels and ownership, and flags non github-nango or missing integrations', async () => {
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(nangoIntegration([]))
    mocks.getReposForGithubIntegration.mockResolvedValue(
      ['New', 'Kept', 'NangoOnly'].map((name) => ({ url: `https://github.com/Acme/${name}` })),
    )
    mocks.getNangoMappingsForIntegrations.mockResolvedValue({
      'int-1': { 'conn-1': { owner: 'Acme', repoName: 'NangoOnly', repositoryId: 'r-1' } },
    })
    mocks.listSyncUnitChannelsForIntegration.mockResolvedValue([
      { channelName: 'https://github.com/Acme/Kept', hasDecommissioned: false, hasEmitting: true },
      {
        channelName: 'https://github.com/Acme/Removed',
        hasDecommissioned: false,
        hasEmitting: true,
      },
    ])
    mocks.listGithubChannelsOwnedElsewhere.mockResolvedValue([])

    await expect(planGithubLifecycle({ integrationId: 'int-1' })).resolves.toEqual({
      found: true,
      githubNango: true,
      deleted: false,
      desired: 3,
      toCreate: ['https://github.com/acme/new'],
      toDeleteKeys: ['https://github.com/acme/removed'],
      invalidUrls: [],
      ownedElsewhere: [],
      deletesSuppressed: false,
    })
    expect(mocks.listGithubChannelsOwnedElsewhere).toHaveBeenCalledWith(mocks.qx, 'int-1', [
      'https://github.com/acme/new',
      'https://github.com/acme/kept',
      'https://github.com/acme/nangoonly',
    ])

    const nothing = {
      desired: 0,
      toCreate: [],
      toDeleteKeys: [],
      invalidUrls: [],
      ownedElsewhere: [],
      deletesSuppressed: false,
    }
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue({
      ...nangoIntegration([]),
      platform: 'github',
    })
    await expect(planGithubLifecycle({ integrationId: 'int-1' })).resolves.toEqual({
      found: true,
      githubNango: false,
      deleted: false,
      ...nothing,
    })

    mocks.fetchGithubLifecycleIntegration.mockResolvedValue(null)
    await expect(planGithubLifecycle({ integrationId: 'int-1' })).resolves.toEqual({
      found: false,
      githubNango: false,
      deleted: false,
      ...nothing,
    })
    expect(mocks.getReposForGithubIntegration).toHaveBeenCalledTimes(1)
  })
})
