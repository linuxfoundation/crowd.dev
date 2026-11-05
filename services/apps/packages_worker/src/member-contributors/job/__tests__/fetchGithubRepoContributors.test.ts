import { heartbeat } from '@temporalio/activity'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { githubApiGet } from '../../../security-contacts/githubToken'
import { fetchGithubRepoContributors } from '../fetchGithubRepoContributors'

vi.mock('@temporalio/activity', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@temporalio/activity')>()),
  heartbeat: vi.fn(),
}))
vi.mock('../../../config', () => ({ getGithubAppConfig: vi.fn(() => ({ appId: '1' })) }))
vi.mock('../../../security-contacts/githubToken', () => ({ githubApiGet: vi.fn() }))

const repo = { repoUrl: 'https://github.com/a/b' }

function contributorsJson(entries: Array<{ login: string; type?: string }>): string {
  return JSON.stringify(entries.map((e) => ({ type: 'User', ...e })))
}

beforeEach(() => {
  vi.clearAllMocks()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('fetchGithubRepoContributors', () => {
  it('returns the lowercased human logins of the first contributors page', async () => {
    vi.mocked(githubApiGet).mockResolvedValue({
      status: 200,
      text: contributorsJson([
        { login: 'Octo' },
        { login: 'dependabot[bot]', type: 'Bot' },
        { login: 'ada' },
      ]),
    })
    await expect(
      fetchGithubRepoContributors({ repoUrl: 'https://github.com/haskell/aeson' }),
    ).resolves.toEqual({ status: 'fetched', logins: ['octo', 'ada'] })
    expect(vi.mocked(githubApiGet).mock.calls).toEqual([
      ['/repos/haskell/aeson/contributors?per_page=100&page=1', 15000, { extraOkStatuses: [204] }],
    ])
  })

  it.each([
    ['an empty repository', { status: 204, text: null }],
    [
      'only bot contributors',
      { status: 200, text: contributorsJson([{ login: 'x[bot]', type: 'Bot' }]) },
    ],
  ])('returns empty for %s', async (_label, response) => {
    vi.mocked(githubApiGet).mockResolvedValue(response)
    await expect(fetchGithubRepoContributors(repo)).resolves.toEqual({
      status: 'empty',
      logins: [],
    })
  })

  it.each([
    [
      'without retry when the repo is missing',
      () => vi.mocked(githubApiGet).mockResolvedValue({ status: 404, text: null }),
      { message: 'Repository not found on GitHub', nonRetryable: true },
    ],
    [
      'retryably when the fetch errors',
      () => vi.mocked(githubApiGet).mockRejectedValue(new Error('socket hang up')),
      {
        message: 'GitHub contributors fetch failed for https://github.com/a/b',
        nonRetryable: false,
      },
    ],
  ])('fails %s', async (_label, arrange, expected) => {
    arrange()
    const error = (await fetchGithubRepoContributors(repo).catch((e: unknown) => e)) as Error & {
      nonRetryable?: boolean
    }
    expect({ message: error.message, nonRetryable: error.nonRetryable === true }).toEqual(expected)
  })

  it('heartbeats while the fetch is in flight and stops once it settles', async () => {
    vi.useFakeTimers()
    vi.mocked(githubApiGet).mockImplementation(
      () =>
        new Promise((resolve) => setTimeout(() => resolve({ status: 204, text: null }), 45_000)),
    )
    const pending = fetchGithubRepoContributors(repo)
    await vi.advanceTimersByTimeAsync(30_000)
    expect(heartbeat).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(15_000)
    await expect(pending).resolves.toEqual({ status: 'empty', logins: [] })
    expect(vi.getTimerCount()).toBe(0)
  })
})
