import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  fetchPublicRepoMetrics,
  fetchPublicRepoReadme,
  getGithubToken,
  parseGithubUrl,
} from './publicRepoClient'
import { GithubPublicClientError } from './types'

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), { status, headers })
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.unstubAllEnvs()
})

describe('parseGithubUrl', () => {
  it('extracts owner and repo name', () => {
    expect(parseGithubUrl('https://github.com/octocat/Hello-World')).toEqual({
      owner: 'octocat',
      name: 'Hello-World',
    })
  })

  it('throws on a non-GitHub URL', () => {
    expect(() => parseGithubUrl('https://gitlab.com/foo/bar')).toThrow(GithubPublicClientError)
  })
})

describe('getGithubToken', () => {
  it('throws CONFIG when the env var is missing', () => {
    vi.stubEnv('CROWD_PROJECT_EVALUATION_GITHUB_TOKEN', '')
    expect(() => getGithubToken()).toThrow(/CROWD_PROJECT_EVALUATION_GITHUB_TOKEN/)
  })

  it('returns the token when set', () => {
    vi.stubEnv('CROWD_PROJECT_EVALUATION_GITHUB_TOKEN', 'test-token')
    expect(getGithubToken()).toBe('test-token')
  })
})

describe('fetchPublicRepoMetrics', () => {
  it('maps a successful GraphQL response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        jsonResponse(200, {
          data: {
            repository: {
              description: 'A repo',
              primaryLanguage: { name: 'TypeScript' },
              stargazerCount: 10,
              forkCount: 2,
              pushedAt: '2026-01-01T00:00:00Z',
              createdAt: '2020-01-01T00:00:00Z',
              isArchived: false,
              isFork: false,
              openIssues: { totalCount: 3 },
              closedIssues: { totalCount: 7 },
              openPullRequests: { totalCount: 1 },
              closedPullRequests: { totalCount: 20 },
            },
          },
        }),
      ),
    )

    const metrics = await fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok')

    expect(metrics).toEqual({
      description: 'A repo',
      primaryLanguage: 'TypeScript',
      stars: 10,
      forks: 2,
      openIssues: 3,
      closedIssues: 7,
      openPullRequests: 1,
      closedPullRequests: 20,
      pushedAt: '2026-01-01T00:00:00Z',
      createdAt: '2020-01-01T00:00:00Z',
      isArchived: false,
      isFork: false,
    })
  })

  it('throws AUTH on 401 instead of failing silently', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 401 })))

    await expect(
      fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'bad-token'),
    ).rejects.toMatchObject({ kind: 'AUTH' })
  })

  it('throws RATE_LIMIT on a rate-limited 403', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response('API rate limit exceeded', {
          status: 403,
          headers: { 'x-ratelimit-reset': String(Math.floor(Date.now() / 1000) + 60) },
        }),
      ),
    )

    await expect(
      fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok'),
    ).rejects.toMatchObject({ kind: 'RATE_LIMIT' })
  })

  it('throws NOT_FOUND on 404', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 404 })))

    await expect(
      fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok'),
    ).rejects.toMatchObject({ kind: 'NOT_FOUND' })
  })
})

describe('fetchPublicRepoReadme', () => {
  it('returns the raw content when present', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('# Hello', { status: 200 })))

    const readme = await fetchPublicRepoReadme('https://github.com/octocat/Hello-World', 'tok')

    expect(readme).toEqual({ content: '# Hello', truncated: false })
  })

  it('returns null when the repo has no README', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 404 })))

    const readme = await fetchPublicRepoReadme('https://github.com/octocat/Hello-World', 'tok')

    expect(readme).toBeNull()
  })

  it('truncates content past maxChars', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('0123456789', { status: 200 })))

    const readme = await fetchPublicRepoReadme('https://github.com/octocat/Hello-World', 'tok', 5)

    expect(readme).toEqual({ content: '01234', truncated: true })
  })

  it('throws AUTH on 401 instead of failing silently', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 401 })))

    await expect(
      fetchPublicRepoReadme('https://github.com/octocat/Hello-World', 'bad-token'),
    ).rejects.toMatchObject({ kind: 'AUTH' })
  })
})
