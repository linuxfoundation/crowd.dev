import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { syncGithubRepoAvailability } from './githubRepoAvailability'

const mocks = vi.hoisted(() => ({
  listUnavailableRepoUrlsForIntegration: vi.fn(),
  listSyncUnitChannelNamesByErrorClass: vi.fn(),
  markRepositoriesUnavailable: vi.fn(),
  clearRepositoriesUnavailable: vi.fn(),
  resolveRepo: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  REPOSITORY_NOT_FOUND_ERROR_CLASS: 'provider.repository_not_found',
  listSyncUnitChannelNamesByErrorClass: mocks.listSyncUnitChannelNamesByErrorClass,
}))
vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  listUnavailableRepoUrlsForIntegration: mocks.listUnavailableRepoUrlsForIntegration,
  markRepositoriesUnavailable: mocks.markRepositoriesUnavailable,
  clearRepositoriesUnavailable: mocks.clearRepositoriesUnavailable,
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

describe('syncGithubRepoAvailability', () => {
  const token = vi.fn(async () => 'token')

  beforeEach(() => {
    vi.clearAllMocks()
    mocks.markRepositoriesUnavailable.mockImplementation(async (_qx, urls: string[]) => urls)
    mocks.clearRepositoriesUnavailable.mockImplementation(async (_qx, urls: string[]) => urls)
  })

  it('flags suspects github confirms gone, clears flagged repos that answer again and leaves every other answer alone', async () => {
    mocks.listUnavailableRepoUrlsForIntegration.mockResolvedValue(
      ['StillGone', 'Back', 'FlakyFlagged'].map(url),
    )
    mocks.listSyncUnitChannelNamesByErrorClass.mockResolvedValue(
      ['Deleted', 'Legal', 'Removed', 'Transient', 'Forbidden', 'StillGone'].map(url),
    )
    githubAnswers({
      StillGone: 404,
      FlakyFlagged: 502,
      Deleted: 404,
      Legal: 451,
      Removed: 410,
      Forbidden: 403,
    })

    await expect(syncGithubRepoAvailability(qx, token, 'int-1')).resolves.toEqual({
      flagged: ['Deleted', 'Legal', 'Removed'].map(url),
      cleared: [url('Back')],
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
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(8)
    expect(mocks.resolveRepo).toHaveBeenCalledWith('token', 'Acme', 'Back')
  })

  it('stops probing on a rate limit, reports a failed mint as skips and does nothing without candidates', async () => {
    const names = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    mocks.listUnavailableRepoUrlsForIntegration.mockResolvedValue([])
    mocks.listSyncUnitChannelNamesByErrorClass.mockResolvedValue(names.map(url))
    githubAnswers({ A: 404, B: 429, F: 404 })

    await expect(syncGithubRepoAvailability(qx, token, 'int-1')).resolves.toEqual({
      flagged: [url('A')],
      cleared: [],
      skipped: [{ url: url('B'), reason: 'rate_limited', message: '429' }],
    })
    expect(mocks.resolveRepo).toHaveBeenCalledTimes(5)

    const failedMint = vi.fn(async () => {
      throw new Error('mint failed')
    })
    await expect(syncGithubRepoAvailability(qx, failedMint, 'int-1')).resolves.toEqual({
      flagged: [],
      cleared: [],
      skipped: names.map((name) => ({
        url: url(name),
        reason: 'token_mint_failed',
        message: 'mint failed',
      })),
    })

    vi.clearAllMocks()
    mocks.listSyncUnitChannelNamesByErrorClass.mockResolvedValue([])
    await expect(syncGithubRepoAvailability(qx, token, 'int-1')).resolves.toEqual({
      flagged: [],
      cleared: [],
      skipped: [],
    })
    expect(token).not.toHaveBeenCalled()
    expect(mocks.markRepositoriesUnavailable).not.toHaveBeenCalled()
  })
})
