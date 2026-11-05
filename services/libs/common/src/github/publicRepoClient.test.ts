import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  fetchPublicOrgRepos,
  fetchPublicRepoMetrics,
  fetchPublicRepoReadme,
  getGithubToken,
  isPublicGithubRepo,
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
      name: 'hello-world',
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

describe('fetchPublicOrgRepos', () => {
  const page = (names: string[], extra: Partial<{ archived: boolean; fork: boolean }> = {}) =>
    names.map((name) => ({ name, archived: false, fork: false, ...extra }))

  it('pages through the organization and leaves archived repositories and forks out', async () => {
    const firstPage = page(Array.from({ length: 100 }, (_, index) => `repo-${index + 1}`))
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(200, firstPage))
      .mockResolvedValueOnce(
        jsonResponse(200, [
          ...page(['Extra']),
          ...page(['old'], { archived: true }),
          ...page(['mirror'], { fork: true }),
        ]),
      )
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchPublicOrgRepos('openbmc', 'token', 500)

    expect(result.exceededLimit).toBe(false)
    expect(result.repoUrls).toHaveLength(101)
    expect(result.repoUrls[100]).toBe('https://github.com/openbmc/extra')
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://api.github.com/orgs/openbmc/repos?type=public&per_page=100&page=1',
    )
  })

  it('stops at the limit and reports that the organization is larger', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(200, page(['a', 'b', 'c']))))

    expect(await fetchPublicOrgRepos('openbmc', 'token', 2)).toEqual({
      repoUrls: ['https://github.com/openbmc/a', 'https://github.com/openbmc/b'],
      exceededLimit: true,
    })
  })

  it('throws NOT_FOUND for an unknown organization and RATE_LIMIT on 429', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 404 })))
    await expect(fetchPublicOrgRepos('nobody', 'token', 20)).rejects.toMatchObject({
      kind: 'NOT_FOUND',
    })

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 429 })))
    await expect(fetchPublicOrgRepos('openbmc', 'token', 20)).rejects.toMatchObject({
      kind: 'RATE_LIMIT',
    })
  })
})

describe('isPublicGithubRepo', () => {
  it('is true for a public repository and false for a private one', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(200, { private: false }))
      .mockResolvedValueOnce(jsonResponse(200, { private: true }))
    vi.stubGlobal('fetch', fetchMock)

    expect(await isPublicGithubRepo('https://github.com/openbmc/bmcweb', 'token')).toBe(true)
    expect(await isPublicGithubRepo('https://github.com/openbmc/secret', 'token')).toBe(false)
    expect(fetchMock.mock.calls[0][0]).toBe('https://api.github.com/repos/openbmc/bmcweb')
  })

  it('is false on 404 and throws RATE_LIMIT on 429', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 404 })))
    expect(await isPublicGithubRepo('https://github.com/org/repo', 'token')).toBe(false)

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 429 })))
    await expect(isPublicGithubRepo('https://github.com/org/repo', 'token')).rejects.toMatchObject({
      kind: 'RATE_LIMIT',
    })
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
              isFork: true,
              parent: { nameWithOwner: 'upstream/Hello-World' },
              hasIssuesEnabled: false,
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
      hasIssuesEnabled: false,
      openPullRequests: 1,
      closedPullRequests: 20,
      pushedAt: '2026-01-01T00:00:00Z',
      createdAt: '2020-01-01T00:00:00Z',
      isArchived: false,
      isFork: true,
      parentFullName: 'upstream/Hello-World',
    })
  })

  it('leaves the parent empty for a repository that is not a fork', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        jsonResponse(200, {
          data: {
            repository: {
              description: null,
              primaryLanguage: null,
              stargazerCount: 0,
              forkCount: 0,
              pushedAt: null,
              createdAt: '2020-01-01T00:00:00Z',
              isArchived: false,
              isFork: false,
              parent: null,
              hasIssuesEnabled: true,
              openIssues: { totalCount: 0 },
              closedIssues: { totalCount: 0 },
              openPullRequests: { totalCount: 0 },
              closedPullRequests: { totalCount: 0 },
            },
          },
        }),
      ),
    )

    const metrics = await fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok')

    expect(metrics.isFork).toBe(false)
    expect(metrics.parentFullName).toBeNull()
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

  it('throws RATE_LIMIT on 429 regardless of body', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response('secondary rate limit', {
          status: 429,
          headers: { 'retry-after': '30' },
        }),
      ),
    )

    await expect(
      fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok'),
    ).rejects.toMatchObject({ kind: 'RATE_LIMIT' })
  })

  it('throws TRANSIENT when fetch itself rejects', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('fetch failed')))

    await expect(
      fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok'),
    ).rejects.toMatchObject({ kind: 'TRANSIENT' })
  })

  it('passes the abort signal to fetch and reports an aborted body read as TRANSIENT', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: () => Promise.reject(new DOMException('aborted', 'AbortError')),
    })
    vi.stubGlobal('fetch', fetchMock)
    const signal = AbortSignal.timeout(1000)

    await expect(
      fetchPublicRepoMetrics('https://github.com/octocat/Hello-World', 'tok', signal),
    ).rejects.toMatchObject({ kind: 'TRANSIENT' })
    expect(fetchMock).toHaveBeenCalledWith(
      'https://api.github.com/graphql',
      expect.objectContaining({ signal }),
    )
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

  it('throws RATE_LIMIT on 429 regardless of body', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValue(new Response('', { status: 429, headers: { 'retry-after': '30' } })),
    )

    await expect(
      fetchPublicRepoReadme('https://github.com/octocat/Hello-World', 'tok'),
    ).rejects.toMatchObject({ kind: 'RATE_LIMIT' })
  })

  it('throws TRANSIENT when fetch itself rejects', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('fetch failed')))

    await expect(
      fetchPublicRepoReadme('https://github.com/octocat/Hello-World', 'tok'),
    ).rejects.toMatchObject({ kind: 'TRANSIENT' })
  })
})
