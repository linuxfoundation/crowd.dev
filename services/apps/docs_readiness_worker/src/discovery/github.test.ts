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

const refs = (urls: string[]) => urls.map((url) => ({ url, starCount: null }))
const repo = (url: string, starCount: number | null) => ({ url, starCount })

describe('primaryRepo', () => {
  it('prefers a github.com URL with owner and repo segments', () => {
    expect(
      primaryRepo(refs(['https://gitlab.com/foo/bar', 'https://github.com/torvalds/linux'])),
    ).toBe('https://github.com/torvalds/linux')
  })

  it('falls back to the first github.com URL when none has both segments', () => {
    expect(primaryRepo(refs(['https://github.com/torvalds', 'https://gitlab.com/foo/bar']))).toBe(
      'https://github.com/torvalds',
    )
  })

  it('ignores non-github URLs entirely', () => {
    expect(primaryRepo(refs(['https://gitlab.com/foo/bar']))).toBeNull()
  })

  it('returns null when the list is empty', () => {
    expect(primaryRepo(refs([]))).toBeNull()
  })

  it('does not let a non-github host matching the substring shadow a real github.com entry', () => {
    expect(
      primaryRepo(refs(['https://notgithub.com/a/b/c', 'https://github.com/torvalds/linux'])),
    ).toBe('https://github.com/torvalds/linux')
  })

  it('ignores a non-github host even when no real github.com entry exists', () => {
    expect(primaryRepo(refs(['https://notgithub.com/a/b/c']))).toBeNull()
  })

  it('ignores malformed repo strings without throwing', () => {
    expect(primaryRepo(refs(['not a url', 'https://github.com/torvalds/linux']))).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('accepts an scp-style git@github.com: url', () => {
    expect(primaryRepo(refs(['git@github.com:torvalds/linux.git']))).toBe(
      'git@github.com:torvalds/linux.git',
    )
  })

  it('accepts a www.github.com url', () => {
    expect(primaryRepo(refs(['https://www.github.com/torvalds/linux']))).toBe(
      'https://www.github.com/torvalds/linux',
    )
  })

  it('accepts a scheme-less github.com url', () => {
    expect(primaryRepo(refs(['github.com/torvalds/linux']))).toBe('github.com/torvalds/linux')
  })

  it('picks a parseable entry over an earlier bare-org url, regardless of slash count', () => {
    expect(
      primaryRepo(refs(['https://github.com/torvalds', 'git@github.com:torvalds/linux.git'])),
    ).toBe('git@github.com:torvalds/linux.git')
  })

  it('accepts an ssh:// scp-style git@github.com: url without a port', () => {
    expect(primaryRepo(refs(['ssh://git@github.com:torvalds/linux.git']))).toBe(
      'ssh://git@github.com:torvalds/linux.git',
    )
  })

  it('does not mistake a real ssh port for a scp-style path separator', () => {
    expect(primaryRepo(refs(['ssh://git@github.com:2222/torvalds/linux.git']))).toBe(
      'ssh://git@github.com:2222/torvalds/linux.git',
    )
  })

  it('prefers a repo named after the slug over higher stars, in any order', () => {
    const liboqsJs = repo('https://github.com/open-quantum-safe/liboqs-js', 500)
    const liboqs = repo('https://github.com/open-quantum-safe/liboqs', 100)
    const hint = { slug: 'liboqs', name: 'Open Quantum Safe' }
    expect(primaryRepo([liboqsJs, liboqs], hint)).toBe(liboqs.url)
    expect(primaryRepo([liboqs, liboqsJs], hint)).toBe(liboqs.url)
  })

  it('matches the normalized project name when the slug does not match', () => {
    const pre = repo('https://github.com/urunc-dev/urunc-pre', 900)
    const main = repo('https://github.com/urunc-dev/urunc', 50)
    expect(primaryRepo([pre, main], { slug: 'unikernel-container', name: 'URUNC' })).toBe(main.url)
  })

  it('falls back to the highest star count when no repo name matches', () => {
    const dot = repo('https://github.com/project-zot/.project', 3)
    const zot = repo('https://github.com/project-zot/zot', 2500)
    expect(primaryRepo([dot, zot], { slug: 'other', name: 'Other' })).toBe(zot.url)
  })

  it('breaks star ties by url ascending', () => {
    const b = repo('https://github.com/org/b', 10)
    const a = repo('https://github.com/org/a', 10)
    expect(primaryRepo([b, a])).toBe(a.url)
  })

  it('sorts unsnapshotted repos last and orders an all-null list by url', () => {
    const none = repo('https://github.com/org/a', null)
    const zero = repo('https://github.com/org/z', 0)
    expect(primaryRepo([none, zero])).toBe(zero.url)
    expect(primaryRepo([repo('https://github.com/org/b', null), none])).toBe(none.url)
  })

  it('picks the slug-named repo in a large multi-repo project (DAOS shape)', () => {
    const repos = [
      repo('https://github.com/daos-stack/daos-docs', 2000),
      repo('https://github.com/daos-stack/daos', 800),
      repo('https://github.com/daos-stack/go-spdk', 3000),
    ]
    expect(primaryRepo(repos, { slug: 'daos', name: 'DAOS' })).toBe(
      'https://github.com/daos-stack/daos',
    )
  })

  it('orders several unparseable github urls by stars then url, not input order', () => {
    expect(
      primaryRepo([repo('https://github.com/zeta', null), repo('https://github.com/alpha', null)]),
    ).toBe('https://github.com/alpha')
    expect(
      primaryRepo([repo('https://github.com/alpha', 1), repo('https://github.com/zeta', 9)]),
    ).toBe('https://github.com/zeta')
  })

  it('keeps a name match among several name-matching repos ordered by stars', () => {
    const small = repo('https://github.com/a/proj', 1)
    const big = repo('https://github.com/b/proj', 9)
    expect(primaryRepo([small, big], { slug: 'proj' })).toBe(big.url)
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
