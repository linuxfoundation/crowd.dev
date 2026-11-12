import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { probeGithubRepoAvailability } from './githubRepoAvailability'

const mocks = vi.hoisted(() => ({
  getReposForGithubIntegration: vi.fn(),
  listUnavailableRepoUrlsForIntegration: vi.fn(),
  listSyncUnitChannelNamesByErrorClass: vi.fn(),
  resolveRepo: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/connectors', async (importActual) => ({
  REPOSITORY_NOT_FOUND_ERROR_CLASS: (
    await importActual<typeof import('@crowd/data-access-layer/src/connectors')>()
  ).REPOSITORY_NOT_FOUND_ERROR_CLASS,
  listSyncUnitChannelNamesByErrorClass: mocks.listSyncUnitChannelNamesByErrorClass,
}))
vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  getReposForGithubIntegration: mocks.getReposForGithubIntegration,
  listUnavailableRepoUrlsForIntegration: mocks.listUnavailableRepoUrlsForIntegration,
}))
vi.mock('@crowd/connectors/src/connectors/github/discover', () => ({
  resolveRepo: mocks.resolveRepo,
}))

const qx = {} as QueryExecutor
const url = (name: string) => `https://github.com/Acme/${name}`
const httpError = (status: number, headers: Record<string, string> = {}) =>
  new axios.AxiosError(String(status), undefined, undefined, undefined, {
    status,
    headers,
  } as never)

function githubAnswers(answers: Record<string, number>): void {
  mocks.resolveRepo.mockImplementation(async (_token: string, _owner: string, name: string) => {
    const status = answers[name] ?? 200
    if (status !== 200) {
      throw httpError(status, status === 429 ? { 'retry-after': '60' } : {})
    }
    return { channelId: `id-${name}`, channelName: url(name), forkedFrom: null }
  })
}

function tracked(names: string[], unavailable: string[], suspectChannels: string[]): void {
  mocks.getReposForGithubIntegration.mockResolvedValue(names.map((name) => ({ url: url(name) })))
  mocks.listUnavailableRepoUrlsForIntegration.mockResolvedValue(unavailable.map(url))
  mocks.listSyncUnitChannelNamesByErrorClass.mockResolvedValue(suspectChannels)
}

describe('probeGithubRepoAvailability', () => {
  const token = vi.fn(async () => 'token')
  const onProgress = vi.fn()

  beforeEach(() => vi.clearAllMocks())

  it('reports own suspects github confirms gone and flagged repos that answer again, by every stored spelling of their url, and leaves every other answer alone', async () => {
    tracked(
      [
        'StillGone',
        'Back',
        'FlakyFlagged',
        'Deleted',
        'Legal',
        'legal',
        'Removed',
        'Transient',
        'Forbidden',
      ],
      ['StillGone', 'Back', 'FlakyFlagged'],
      [
        url('Deleted').toLowerCase(),
        url('Legal'),
        url('Removed'),
        url('Transient'),
        url('Forbidden'),
        url('StillGone'),
        url('OwnedElsewhere'),
        'not-a-repo-url',
      ],
    )
    githubAnswers({
      StillGone: 404,
      FlakyFlagged: 502,
      Deleted: 404,
      Legal: 451,
      legal: 451,
      Removed: 410,
      Forbidden: 403,
      OwnedElsewhere: 404,
    })

    await expect(probeGithubRepoAvailability(qx, token, 'int-1', onProgress)).resolves.toEqual({
      gone: ['Deleted', 'Legal', 'legal', 'Removed'].map(url),
      reachable: [url('Back')],
      skipped: [
        { url: url('FlakyFlagged'), reason: 'github_error', message: '502' },
        { url: url('Forbidden'), reason: 'github_error', message: '403' },
      ],
    })
    expect(mocks.listSyncUnitChannelNamesByErrorClass).toHaveBeenCalledWith(
      qx,
      'int-1',
      'provider.repository_not_found',
    )
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(9)
    expect(mocks.resolveRepo).not.toHaveBeenCalledWith('token', 'Acme', 'OwnedElsewhere')
    expect(onProgress).toHaveBeenCalledTimes(2)
  })

  it('reports what it could not probe after a rate limit or a failed mint and keeps earlier answers', async () => {
    const names = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    tracked(names, [], names.map(url))
    githubAnswers({ A: 404, B: 429, F: 404 })

    await expect(probeGithubRepoAvailability(qx, token, 'int-1', onProgress)).resolves.toEqual({
      gone: [url('A')],
      reachable: [],
      skipped: [
        { url: url('B'), reason: 'rate_limited', message: '429' },
        ...['F', 'G'].map((name) => ({
          url: url(name),
          reason: 'rate_limited',
          message: 'not probed after a rate limit',
        })),
      ],
    })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(5)

    githubAnswers({ A: 404 })
    const expiringToken = vi
      .fn<() => Promise<string>>()
      .mockResolvedValueOnce('token')
      .mockRejectedValue(new Error('mint failed'))
    await expect(
      probeGithubRepoAvailability(qx, expiringToken, 'int-1', onProgress),
    ).resolves.toEqual({
      gone: [url('A')],
      reachable: [],
      skipped: ['F', 'G'].map((name) => ({
        url: url(name),
        reason: 'token_mint_failed',
        message: 'mint failed',
      })),
    })

    vi.clearAllMocks()
    tracked(names, [], [])
    await expect(probeGithubRepoAvailability(qx, token, 'int-1', onProgress)).resolves.toEqual({
      gone: [],
      reachable: [],
      skipped: [],
    })
    expect(token).not.toHaveBeenCalled()
  })
})
