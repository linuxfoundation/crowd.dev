import { afterEach, describe, expect, it, vi } from 'vitest'

import { getPackageJson, getReadme, getRepoHomepage, parseGithubRepo, primaryRepo } from './github'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('parseGithubRepo', () => {
  it('parses a github.com URL', () => {
    expect(parseGithubRepo('https://github.com/torvalds/linux')).toEqual({
      owner: 'torvalds',
      repo: 'linux',
    })
  })

  it('returns null for a non-github URL', () => {
    expect(parseGithubRepo('https://gitlab.com/torvalds/linux')).toBeNull()
  })
})

describe('primaryRepo', () => {
  it('prefers a github.com URL with owner and repo segments', () => {
    expect(primaryRepo(['https://gitlab.com/foo/bar', 'https://github.com/torvalds/linux'])).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('falls back to the first github.com URL when none has both segments', () => {
    expect(primaryRepo(['https://github.com/torvalds', 'https://gitlab.com/foo/bar'])).toBe(
      'https://github.com/torvalds',
    )
  })

  it('ignores non-github URLs entirely', () => {
    expect(primaryRepo(['https://gitlab.com/foo/bar'])).toBeNull()
  })

  it('returns null when the list is empty', () => {
    expect(primaryRepo([])).toBeNull()
  })

  it('does not let a non-github host matching the substring shadow a real github.com entry', () => {
    expect(primaryRepo(['https://notgithub.com/a/b/c', 'https://github.com/torvalds/linux'])).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('ignores a non-github host even when no real github.com entry exists', () => {
    expect(primaryRepo(['https://notgithub.com/a/b/c'])).toBeNull()
  })

  it('ignores malformed repo strings without throwing', () => {
    expect(primaryRepo(['not a url', 'https://github.com/torvalds/linux'])).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('accepts an scp-style git@github.com: url', () => {
    expect(primaryRepo(['git@github.com:torvalds/linux.git'])).toBe(
      'git@github.com:torvalds/linux.git',
    )
  })

  it('accepts a www.github.com url', () => {
    expect(primaryRepo(['https://www.github.com/torvalds/linux'])).toBe(
      'https://www.github.com/torvalds/linux',
    )
  })

  it('accepts a scheme-less github.com url', () => {
    expect(primaryRepo(['github.com/torvalds/linux'])).toBe('github.com/torvalds/linux')
  })

  it('picks a parseable entry over an earlier bare-org url, regardless of slash count', () => {
    expect(primaryRepo(['https://github.com/torvalds', 'git@github.com:torvalds/linux.git'])).toBe(
      'git@github.com:torvalds/linux.git',
    )
  })

  it('accepts an ssh:// scp-style git@github.com: url without a port', () => {
    expect(primaryRepo(['ssh://git@github.com:torvalds/linux.git'])).toBe(
      'ssh://git@github.com:torvalds/linux.git',
    )
  })

  it('does not mistake a real ssh port for a scp-style path separator', () => {
    expect(primaryRepo(['ssh://git@github.com:2222/torvalds/linux.git'])).toBe(
      'ssh://git@github.com:2222/torvalds/linux.git',
    )
  })
})

function stubFetch(handler: (input: string, init?: RequestInit) => Response) {
  const fetchMock = vi.fn((input: string | URL | Request, init?: RequestInit) =>
    Promise.resolve(handler(typeof input === 'string' ? input : input.toString(), init)),
  )
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

describe('getRepoHomepage', () => {
  it('returns the homepage field on the happy path', async () => {
    const fetchMock = stubFetch(() => Response.json({ homepage: 'https://example.com' }))

    expect(await getRepoHomepage('torvalds', 'linux', 'token123')).toBe('https://example.com')

    const [, init] = fetchMock.mock.calls[0]
    expect(init?.headers).toEqual(
      expect.objectContaining({
        Authorization: 'Bearer token123',
        'X-GitHub-Api-Version': '2022-11-28',
      }),
    )
  })

  it('returns null on a non-ok status', async () => {
    stubFetch(() => new Response('not found', { status: 404 }))
    expect(await getRepoHomepage('torvalds', 'linux', 'token123')).toBeNull()
  })

  it('returns null when fetch throws', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('timeout'))),
    )
    expect(await getRepoHomepage('torvalds', 'linux', 'token123')).toBeNull()
  })
})

describe('getReadme', () => {
  it('returns the raw readme text on the happy path', async () => {
    stubFetch(() => new Response('# Readme'))
    expect(await getReadme('torvalds', 'linux', 'token123')).toBe('# Readme')
  })

  it('returns null on a non-ok status', async () => {
    stubFetch(() => new Response('not found', { status: 404 }))
    expect(await getReadme('torvalds', 'linux', 'token123')).toBeNull()
  })

  it('returns null when fetch throws', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('timeout'))),
    )
    expect(await getReadme('torvalds', 'linux', 'token123')).toBeNull()
  })
})

describe('getPackageJson', () => {
  it('returns documentation/homepage on the happy path', async () => {
    stubFetch(
      () =>
        new Response(
          JSON.stringify({
            documentation: 'https://docs.example.com',
            homepage: 'https://example.com',
          }),
        ),
    )
    expect(await getPackageJson('torvalds', 'linux', 'token123')).toEqual({
      documentation: 'https://docs.example.com',
      homepage: 'https://example.com',
    })
  })

  it('returns null on a non-ok status', async () => {
    stubFetch(() => new Response('not found', { status: 404 }))
    expect(await getPackageJson('torvalds', 'linux', 'token123')).toBeNull()
  })

  it('returns null on malformed JSON', async () => {
    stubFetch(() => new Response('not json'))
    expect(await getPackageJson('torvalds', 'linux', 'token123')).toBeNull()
  })

  it('returns null when fetch throws', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('timeout'))),
    )
    expect(await getPackageJson('torvalds', 'linux', 'token123')).toBeNull()
  })
})
