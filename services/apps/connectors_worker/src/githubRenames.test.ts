import axios from 'axios'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { BackendRequestError } from '@crowd/project-onboarding/src/onboarder'
import type { IGithubOrgSettings } from '@crowd/project-onboarding/src/types'

import {
  RENAME_LOCK_WAIT_MS,
  applyGithubRenames,
  buildRenamePayload,
  hydratedReposMatch,
} from './githubRenames'

const mocks = vi.hoisted(() => ({
  readBackendApiConfig: vi.fn(),
  fetchGithubIntegration: vi.fn(),
  fetchGithubIntegrationMapping: vi.fn(),
  postGithubNangoConnect: vi.fn(),
  fetchGithubOrgLogo: vi.fn(),
  fetchGithubLifecycleIntegration: vi.fn(),
  tryLockGithubRenames: vi.fn(),
  findRepositoryByUrlInsensitive: vi.fn(),
  resolveRepo: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('./main', () => ({ svc: { log: mocks.log } }))
vi.mock('@crowd/project-onboarding/src/onboarder', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/project-onboarding/src/onboarder')>()),
  readBackendApiConfig: mocks.readBackendApiConfig,
  fetchGithubIntegration: mocks.fetchGithubIntegration,
  fetchGithubIntegrationMapping: mocks.fetchGithubIntegrationMapping,
  postGithubNangoConnect: mocks.postGithubNangoConnect,
  fetchGithubOrgLogo: mocks.fetchGithubOrgLogo,
}))
vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  fetchGithubLifecycleIntegration: mocks.fetchGithubLifecycleIntegration,
  tryLockGithubRenames: mocks.tryLockGithubRenames,
}))
vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  findRepositoryByUrlInsensitive: mocks.findRepositoryByUrlInsensitive,
}))
vi.mock('@crowd/connectors/src/connectors/github/discover', () => ({
  resolveRepo: mocks.resolveRepo,
}))

const NOW = new Date('2026-11-03T10:00:00Z')
const txQx = { kind: 'tx' }
const qx = { tx: vi.fn(async (fn: (tx: unknown) => unknown) => fn(txQx)) } as never
const token = vi.fn(async () => 'ghs_token')

const repo = (url: string) => ({
  name: url.split('/').pop() ?? '',
  url,
  forkedFrom: null,
  updatedAt: '2026-01-01T00:00:00.000Z',
})

const org = (name: string, urls: string[]): IGithubOrgSettings => ({
  name,
  url: `https://github.com/${name}`,
  logo: `logo-${name}`,
  fullSync: true,
  updatedAt: '2026-01-01T00:00:00.000Z',
  repos: urls.map(repo),
})

const httpError = (status: number) =>
  new axios.AxiosError(String(status), String(status), undefined, undefined, {
    status,
    headers: {},
  } as never)

const eventsOf = (level: 'info' | 'warn' | 'error', event: string) =>
  mocks.log[level].mock.calls.map(([fields]) => fields).filter((fields) => fields.event === event)

describe('hydratedReposMatch', () => {
  it('is case-insensitive and fails on an extra or missing repo', () => {
    const orgs = [org('Acme', ['https://github.com/Acme/One', 'https://github.com/Acme/Two'])]
    const mapping = { 'https://github.com/acme/one': 's1', 'https://github.com/acme/two': 's1' }

    expect(hydratedReposMatch(orgs, mapping)).toBe(true)
    expect(hydratedReposMatch(orgs, { ...mapping, 'https://github.com/acme/three': 's1' })).toBe(
      false,
    )
    expect(hydratedReposMatch(orgs, { 'https://github.com/acme/one': 's1' })).toBe(false)
  })
})

describe('buildRenamePayload', () => {
  it('swaps a rename onto the old segment and keeps every untouched repo on its segment', () => {
    const payload = buildRenamePayload({
      integrationId: 'int-1',
      segmentId: 'seg',
      orgs: [org('org', ['https://github.com/org/old', 'https://github.com/org/b'])],
      mapping: { 'https://github.com/org/old': 'sub1', 'https://github.com/org/b': 'sub2' },
      decisions: [
        {
          rename: { from: 'https://github.com/org/old', to: 'https://github.com/org/New' },
          action: 'swapped',
          forkedFrom: null,
          orgLogo: '',
        },
      ],
      now: NOW,
    }) as {
      integrationId: string
      segments: string[]
      mapping: object
      settings: { orgs: IGithubOrgSettings[] }
    }

    expect(payload.mapping).toEqual({
      'https://github.com/org/New': 'sub1',
      'https://github.com/org/b': 'sub2',
    })
    expect(payload.settings.orgs).toHaveLength(1)
    expect(payload.settings.orgs[0].repos.map((r) => r.url)).toEqual([
      'https://github.com/org/b',
      'https://github.com/org/New',
    ])
    expect(payload.integrationId).toBe('int-1')
    expect(payload.segments).toEqual(['seg'])
  })

  it('adds a new org for a transferred repo', () => {
    const payload = buildRenamePayload({
      integrationId: 'int-1',
      segmentId: 'seg',
      orgs: [org('org-a', ['https://github.com/org-a/x'])],
      mapping: { 'https://github.com/org-a/x': 'sub1' },
      decisions: [
        {
          rename: { from: 'https://github.com/org-a/x', to: 'https://github.com/org-b/x' },
          action: 'swapped',
          forkedFrom: 'https://github.com/up/x',
          orgLogo: 'L',
        },
      ],
      now: NOW,
    }) as { mapping: object; settings: { orgs: IGithubOrgSettings[] } }

    expect(payload.settings.orgs).toEqual([
      { ...org('org-a', []) },
      {
        name: 'org-b',
        url: 'https://github.com/org-b',
        logo: 'L',
        fullSync: false,
        updatedAt: NOW.toISOString(),
        repos: [
          {
            name: 'x',
            url: 'https://github.com/org-b/x',
            forkedFrom: 'https://github.com/up/x',
            updatedAt: NOW.toISOString(),
          },
        ],
      },
    ])
    expect(payload.mapping).toEqual({ 'https://github.com/org-b/x': 'sub1' })
  })

  it('disconnect drops the repo and adds nothing', () => {
    const payload = buildRenamePayload({
      integrationId: 'int-1',
      segmentId: 'seg',
      orgs: [org('org', ['https://github.com/Org/Old', 'https://github.com/org/b'])],
      mapping: { 'https://github.com/org/old': 'sub1', 'https://github.com/org/b': 'sub2' },
      decisions: [
        {
          rename: { from: 'https://github.com/org/old', to: 'https://github.com/other/new' },
          action: 'disconnected',
          forkedFrom: null,
          orgLogo: '',
        },
      ],
      now: NOW,
    }) as { mapping: object; settings: { orgs: IGithubOrgSettings[] } }

    expect(payload.settings.orgs).toEqual([org('org', ['https://github.com/org/b'])])
    expect(payload.mapping).toEqual({ 'https://github.com/org/b': 'sub2' })
  })
})

describe('applyGithubRenames', () => {
  const renameOld = { from: 'https://github.com/acme/old', to: 'https://github.com/acme/New' }
  const renameTwo = { from: 'https://github.com/acme/two', to: 'https://github.com/acme/Deux' }

  const postedPayload = () =>
    mocks.postGithubNangoConnect.mock.calls[0][0] as {
      mapping: Record<string, string>
      settings: { orgs: IGithubOrgSettings[] }
    }

  beforeEach(() => {
    vi.clearAllMocks()
    mocks.readBackendApiConfig.mockReturnValue({ apiUrl: 'http://api', apiToken: 'agent' })
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue({
      id: 'int-1',
      platform: 'github-nango',
      segmentId: 'seg-1',
      settings: null,
      deletedAt: null,
    })
    mocks.fetchGithubIntegration.mockResolvedValue({
      id: 'int-1',
      orgs: [
        org('acme', [
          'https://github.com/acme/old',
          'https://github.com/acme/two',
          'https://github.com/acme/keep',
        ]),
      ],
    })
    mocks.fetchGithubIntegrationMapping.mockResolvedValue({
      'https://github.com/acme/old': 'sub-old',
      'https://github.com/acme/two': 'sub-two',
      'https://github.com/acme/keep': 'sub-keep',
    })
    mocks.findRepositoryByUrlInsensitive.mockResolvedValue(null)
    mocks.resolveRepo.mockImplementation(async (_token: string, owner: string, name: string) => ({
      channelId: `id-${name}`,
      channelName: `https://github.com/${owner}/${name}`,
      forkedFrom: null,
    }))
    mocks.postGithubNangoConnect.mockResolvedValue(undefined)
    mocks.tryLockGithubRenames.mockResolvedValue(true)
  })

  it('returns zeros for no renames without any lookup', async () => {
    await expect(applyGithubRenames(qx, 'int-1', [], token)).resolves.toEqual({
      applied: 0,
      refused: 0,
      failed: 0,
    })
    expect(mocks.fetchGithubLifecycleIntegration).not.toHaveBeenCalled()
  })

  it('applies all renames of an integration in one post', async () => {
    const result = await applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)

    expect(result).toEqual({ applied: 2, refused: 0, failed: 0 })
    expect(mocks.fetchGithubIntegration).toHaveBeenCalledWith('int-1', 'seg-1', {
      apiUrl: 'http://api',
      apiToken: 'agent',
    })
    expect(mocks.fetchGithubIntegrationMapping).toHaveBeenCalledWith(
      'int-1',
      'seg-1',
      'http://api',
      'agent',
    )
    expect(mocks.resolveRepo).toHaveBeenCalledWith('ghs_token', 'acme', 'New')
    expect(mocks.fetchGithubOrgLogo).not.toHaveBeenCalled()
    expect(mocks.postGithubNangoConnect).toHaveBeenCalledTimes(1)
    expect(postedPayload().mapping).toEqual({
      'https://github.com/acme/keep': 'sub-keep',
      'https://github.com/acme/New': 'sub-old',
      'https://github.com/acme/Deux': 'sub-two',
    })
    expect(eventsOf('info', 'github_rename_applied')).toEqual([
      { event: 'github_rename_applied', integrationId: 'int-1', ...renameOld, action: 'swapped' },
      { event: 'github_rename_applied', integrationId: 'int-1', ...renameTwo, action: 'swapped' },
    ])
  })

  it('fetches the logo of a new owner only', async () => {
    mocks.fetchGithubOrgLogo.mockResolvedValue('L')
    const transfer = { from: 'https://github.com/acme/old', to: 'https://github.com/beta/old' }

    await applyGithubRenames(qx, 'int-1', [transfer], token)

    expect(mocks.fetchGithubOrgLogo).toHaveBeenCalledWith('beta', 'ghs_token')
    expect(postedPayload().settings.orgs.map(({ name, logo }) => ({ name, logo }))).toEqual([
      { name: 'acme', logo: 'logo-acme' },
      { name: 'beta', logo: 'L' },
    ])
  })

  it('disconnects only when the new name is already live', async () => {
    mocks.findRepositoryByUrlInsensitive.mockImplementation(async (_qx: unknown, url: string) =>
      url === renameOld.to ? { id: 'repo-live', url } : null,
    )

    const result = await applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)

    expect(result).toEqual({ applied: 2, refused: 0, failed: 0 })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(1)
    expect(Object.keys(postedPayload().mapping)).toEqual([
      'https://github.com/acme/keep',
      'https://github.com/acme/Deux',
    ])
    expect(eventsOf('info', 'github_rename_applied').map(({ action }) => action)).toEqual([
      'disconnected',
      'swapped',
    ])
  })

  it('disconnects with new_name_unreachable on a 404', async () => {
    mocks.resolveRepo.mockRejectedValue(httpError(404))

    const result = await applyGithubRenames(qx, 'int-1', [renameOld], token)

    expect(result).toEqual({ applied: 1, refused: 0, failed: 0 })
    expect(Object.keys(postedPayload().mapping)).toEqual([
      'https://github.com/acme/two',
      'https://github.com/acme/keep',
    ])
    expect(eventsOf('info', 'github_rename_applied')).toEqual([
      expect.objectContaining({ action: 'disconnected', reason: 'new_name_unreachable' }),
    ])
  })

  it('fails only the rename GitHub could not resolve', async () => {
    mocks.resolveRepo.mockImplementation(async (_token: string, owner: string, name: string) => {
      if (name === 'New') throw httpError(502)
      return {
        channelId: 'id',
        channelName: `https://github.com/${owner}/${name}`,
        forkedFrom: null,
      }
    })

    const result = await applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)

    expect(result).toEqual({ applied: 1, refused: 0, failed: 1 })
    expect(Object.keys(postedPayload().mapping)).toContain('https://github.com/acme/old')
    expect(eventsOf('error', 'github_rename_failed')).toEqual([
      expect.objectContaining({ ...renameOld, integrationId: 'int-1', reason: 'github_error' }),
    ])
  })

  it('skips a rename whose old repository is no longer on the integration as a no-op', async () => {
    const removed = { from: 'https://github.com/acme/gone', to: 'https://github.com/acme/Back' }

    const result = await applyGithubRenames(qx, 'int-1', [removed, renameOld], token)

    expect(result).toEqual({ applied: 1, refused: 0, failed: 0 })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(1)
    expect(mocks.findRepositoryByUrlInsensitive).not.toHaveBeenCalledWith(qx, removed.to)
    expect(Object.keys(postedPayload().mapping)).not.toContain(removed.to)
    expect(eventsOf('warn', 'lifecycle_repo_skipped')).toEqual([])
    expect(eventsOf('info', 'github_rename_noop')).toEqual([
      {
        event: 'github_rename_noop',
        integrationId: 'int-1',
        ...removed,
        reason: 'old_repo_not_connected',
      },
    ])
    expect(eventsOf('info', 'github_rename_applied')).toEqual([
      expect.objectContaining({ ...renameOld }),
    ])
  })

  it('refuses on state mismatch without posting', async () => {
    mocks.fetchGithubIntegrationMapping.mockResolvedValue({
      'https://github.com/acme/old': 'sub-old',
    })

    const result = await applyGithubRenames(qx, 'int-1', [renameOld], token)

    expect(result).toEqual({ applied: 0, refused: 1, failed: 0 })
    expect(mocks.postGithubNangoConnect).not.toHaveBeenCalled()
    expect(eventsOf('warn', 'lifecycle_repo_skipped')).toEqual([
      expect.objectContaining({
        integrationId: 'int-1',
        url: renameOld.from,
        reason: 'repo_renamed',
        message: expect.stringContaining('state_mismatch'),
      }),
    ])
  })

  it('posts nothing for a non-live integration', async () => {
    mocks.fetchGithubLifecycleIntegration.mockResolvedValue({
      id: 'int-1',
      platform: 'github-nango',
      segmentId: 'seg-1',
      settings: null,
      deletedAt: '2026-11-01T00:00:00Z',
    })

    const result = await applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)

    expect(result).toEqual({ applied: 0, refused: 2, failed: 0 })
    expect(mocks.fetchGithubIntegration).not.toHaveBeenCalled()
    expect(mocks.postGithubNangoConnect).not.toHaveBeenCalled()
    expect(eventsOf('warn', 'lifecycle_repo_skipped').map(({ message }) => message)).toEqual([
      expect.stringContaining('integration not live'),
      expect.stringContaining('integration not live'),
    ])
  })

  it('fails every rename without config', async () => {
    mocks.readBackendApiConfig.mockReturnValue(null)

    const result = await applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)

    expect(result).toEqual({ applied: 0, refused: 0, failed: 2 })
    expect(mocks.fetchGithubLifecycleIntegration).not.toHaveBeenCalled()
    expect(eventsOf('error', 'github_rename_failed').map(({ reason }) => reason)).toEqual([
      'missing_config',
      'missing_config',
    ])
  })

  it('refuses on a 4xx and fails on a 5xx', async () => {
    mocks.postGithubNangoConnect.mockRejectedValueOnce(
      new BackendRequestError('repo already connected elsewhere', 400),
    )
    await expect(applyGithubRenames(qx, 'int-1', [renameOld], token)).resolves.toEqual({
      applied: 0,
      refused: 1,
      failed: 0,
    })
    expect(eventsOf('warn', 'lifecycle_repo_skipped')).toEqual([
      expect.objectContaining({
        reason: 'repo_renamed',
        message: expect.stringContaining('repo already connected elsewhere'),
      }),
    ])
    expect(eventsOf('error', 'github_rename_failed')).toEqual([])

    vi.clearAllMocks()
    mocks.postGithubNangoConnect.mockRejectedValueOnce(new BackendRequestError('boom', 503))
    await expect(applyGithubRenames(qx, 'int-1', [renameOld], token)).resolves.toEqual({
      applied: 0,
      refused: 0,
      failed: 1,
    })
    expect(eventsOf('warn', 'lifecycle_repo_skipped')).toEqual([])
    expect(eventsOf('error', 'github_rename_failed')).toEqual([
      expect.objectContaining({ ...renameOld, message: 'boom' }),
    ])
    expect(eventsOf('info', 'github_rename_applied')).toEqual([])
  })
  it('serializes per integration and builds the post from the state re-read under the lock', async () => {
    const order: string[] = []
    mocks.tryLockGithubRenames.mockImplementation(async () => {
      order.push('lock')
      return true
    })
    mocks.fetchGithubIntegrationMapping.mockImplementation(async () => {
      order.push('read')
      return order.includes('lock')
        ? {
            'https://github.com/acme/old': 'sub-old',
            'https://github.com/acme/Deux': 'sub-two',
            'https://github.com/acme/keep': 'sub-keep',
          }
        : {
            'https://github.com/acme/old': 'sub-old',
            'https://github.com/acme/two': 'sub-two',
            'https://github.com/acme/keep': 'sub-keep',
          }
    })
    mocks.fetchGithubIntegration.mockImplementation(async () => ({
      id: 'int-1',
      orgs: [
        org('acme', [
          'https://github.com/acme/old',
          order.includes('lock') ? 'https://github.com/acme/Deux' : 'https://github.com/acme/two',
          'https://github.com/acme/keep',
        ]),
      ],
    }))
    mocks.postGithubNangoConnect.mockImplementation(async () => {
      order.push('post')
    })

    const result = await applyGithubRenames(qx, 'int-1', [renameOld], token)

    expect(result).toEqual({ applied: 1, refused: 0, failed: 0 })
    expect(order).toEqual(['read', 'lock', 'read', 'post'])
    expect(mocks.tryLockGithubRenames).toHaveBeenCalledWith(txQx, 'int-1')
    expect(postedPayload().mapping).toEqual({
      'https://github.com/acme/Deux': 'sub-two',
      'https://github.com/acme/keep': 'sub-keep',
      'https://github.com/acme/New': 'sub-old',
    })
  })

  it('re-applies the guard and the not-connected noop to the state read under the lock', async () => {
    let reads = 0
    mocks.fetchGithubIntegration.mockImplementation(async () => {
      reads++
      const urls = ['https://github.com/acme/two', 'https://github.com/acme/keep']
      return {
        id: 'int-1',
        orgs: [org('acme', reads === 1 ? ['https://github.com/acme/old', ...urls] : urls)],
      }
    })
    mocks.fetchGithubIntegrationMapping.mockImplementation(async () => ({
      ...(reads === 1 ? { 'https://github.com/acme/old': 'sub-old' } : {}),
      'https://github.com/acme/two': 'sub-two',
      'https://github.com/acme/keep': 'sub-keep',
    }))

    await expect(applyGithubRenames(qx, 'int-1', [renameOld], token)).resolves.toEqual({
      applied: 0,
      refused: 0,
      failed: 0,
    })
    expect(mocks.postGithubNangoConnect).not.toHaveBeenCalled()
    expect(eventsOf('info', 'github_rename_noop')).toEqual([
      expect.objectContaining({ ...renameOld, reason: 'old_repo_not_connected' }),
    ])

    vi.clearAllMocks()
    reads = 0
    mocks.fetchGithubIntegration.mockImplementation(async () => {
      reads++
      return { id: 'int-1', orgs: [org('acme', ['https://github.com/acme/old'])] }
    })
    mocks.fetchGithubIntegrationMapping.mockImplementation(async () =>
      reads === 1 ? { 'https://github.com/acme/old': 'sub-old' } : {},
    )

    await expect(applyGithubRenames(qx, 'int-1', [renameOld], token)).resolves.toEqual({
      applied: 0,
      refused: 1,
      failed: 0,
    })
    expect(mocks.postGithubNangoConnect).not.toHaveBeenCalled()
    expect(eventsOf('warn', 'lifecycle_repo_skipped')).toEqual([
      expect.objectContaining({ message: expect.stringContaining('state_mismatch') }),
    ])
  })

  it('reports progress once per decided rename and fetches a new owner logo once', async () => {
    mocks.fetchGithubOrgLogo.mockResolvedValue('L')
    const onProgress = vi.fn()
    const moves = ['old', 'two', 'keep'].map((name) => ({
      from: `https://github.com/acme/${name}`,
      to: `https://github.com/beta/${name}`,
    }))
    const gone = { from: 'https://github.com/acme/gone', to: 'https://github.com/beta/gone' }

    const result = await applyGithubRenames(qx, 'int-1', [...moves, gone], token, onProgress)

    expect(result).toEqual({ applied: 3, refused: 0, failed: 0 })
    expect(onProgress).toHaveBeenCalledTimes(3)
    expect(mocks.fetchGithubOrgLogo).toHaveBeenCalledTimes(1)
    expect(postedPayload().settings.orgs.find(({ name }) => name === 'beta')?.logo).toBe('L')
  })

  it('fetches the logo under the lock when the owner left the integration meanwhile', async () => {
    mocks.fetchGithubOrgLogo.mockResolvedValue('L')
    const transfer = { from: 'https://github.com/acme/old', to: 'https://github.com/beta/x' }
    let reads = 0
    mocks.fetchGithubIntegration.mockImplementation(async () => {
      reads++
      return {
        id: 'int-1',
        orgs: [
          org('acme', ['https://github.com/acme/old']),
          ...(reads === 1 ? [org('beta', [])] : []),
        ],
      }
    })
    mocks.fetchGithubIntegrationMapping.mockResolvedValue({ 'https://github.com/acme/old': 's' })

    await applyGithubRenames(qx, 'int-1', [transfer], token)

    expect(mocks.fetchGithubOrgLogo).toHaveBeenCalledTimes(1)
    expect(postedPayload().settings.orgs.find(({ name }) => name === 'beta')?.logo).toBe('L')
  })

  it('connects the url GitHub resolves and checks that url for a live row', async () => {
    const renamed = { from: 'https://github.com/acme/old', to: 'https://github.com/acme/new' }
    mocks.resolveRepo.mockResolvedValue({
      channelId: 'id-new',
      channelName: 'https://github.com/acme/New-Name',
      forkedFrom: null,
    })

    await applyGithubRenames(qx, 'int-1', [renamed], token)

    expect(mocks.findRepositoryByUrlInsensitive).toHaveBeenCalledWith(
      qx,
      'https://github.com/acme/New-Name',
    )
    expect(postedPayload().mapping).toEqual({
      'https://github.com/acme/two': 'sub-two',
      'https://github.com/acme/keep': 'sub-keep',
      'https://github.com/acme/New-Name': 'sub-old',
    })

    vi.clearAllMocks()
    mocks.findRepositoryByUrlInsensitive.mockImplementation(async (_qx: unknown, url: string) =>
      url === 'https://github.com/acme/New-Name' ? { id: 'repo-live', url } : null,
    )

    await applyGithubRenames(qx, 'int-1', [renamed], token)

    expect(Object.keys(postedPayload().mapping)).toEqual([
      'https://github.com/acme/two',
      'https://github.com/acme/keep',
    ])
    expect(eventsOf('info', 'github_rename_applied')).toEqual([
      expect.objectContaining({ action: 'disconnected', reason: 'new_name_already_live' }),
    ])
  })
  describe('when another rename holds the integration lock', () => {
    beforeEach(() => {
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
    })
    afterEach(() => {
      vi.useRealTimers()
    })

    it('waits without holding the lock, heartbeating, and posts once it is free', async () => {
      const onProgress = vi.fn()
      mocks.tryLockGithubRenames
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true)

      const applying = applyGithubRenames(qx, 'int-1', [renameOld], token, onProgress)
      await vi.runAllTimersAsync()

      await expect(applying).resolves.toEqual({ applied: 1, refused: 0, failed: 0 })
      expect(mocks.tryLockGithubRenames).toHaveBeenCalledTimes(3)
      expect(mocks.fetchGithubIntegration).toHaveBeenCalledTimes(2)
      expect(mocks.postGithubNangoConnect).toHaveBeenCalledTimes(1)
      expect(onProgress).toHaveBeenCalledTimes(3)
    })

    it('fails every decided rename with lock_busy at the wait deadline', async () => {
      mocks.tryLockGithubRenames.mockResolvedValue(false)
      const start = Date.now()

      const applying = applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)
      await vi.runAllTimersAsync()

      await expect(applying).resolves.toEqual({ applied: 0, refused: 0, failed: 2 })
      expect(Date.now() - start).toBeLessThanOrEqual(RENAME_LOCK_WAIT_MS)
      expect(mocks.fetchGithubIntegration).toHaveBeenCalledTimes(1)
      expect(mocks.postGithubNangoConnect).not.toHaveBeenCalled()
      expect(eventsOf('error', 'github_rename_failed')).toEqual([
        expect.objectContaining({ ...renameOld, reason: 'lock_busy' }),
        expect.objectContaining({ ...renameTwo, reason: 'lock_busy' }),
      ])
    })
  })

  it('skips a rename GitHub resolves back to the old name as a no-op', async () => {
    mocks.resolveRepo.mockImplementation(async (_token: string, owner: string, name: string) => ({
      channelId: 'id',
      channelName:
        name === 'New' ? 'https://github.com/Acme/Old' : `https://github.com/${owner}/${name}`,
      forkedFrom: null,
    }))

    const result = await applyGithubRenames(qx, 'int-1', [renameOld, renameTwo], token)

    expect(result).toEqual({ applied: 1, refused: 0, failed: 0 })
    expect(postedPayload().mapping).toEqual({
      'https://github.com/acme/old': 'sub-old',
      'https://github.com/acme/keep': 'sub-keep',
      'https://github.com/acme/Deux': 'sub-two',
    })
    expect(eventsOf('info', 'github_rename_noop')).toEqual([
      expect.objectContaining({ ...renameOld, reason: 'renamed_back' }),
    ])
  })
})
