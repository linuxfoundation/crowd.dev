import { afterEach, describe, expect, it, vi } from 'vitest'

import { discoverDocs } from './index'
import {
  docsPath,
  docsSubdomain,
  githubHomepage,
  llmsTxtProbe,
  packageManifest,
  projectWebsite,
  readmeScrape,
  repoUrl,
  serpStrategy,
} from './strategies'

function routeFetch(routes: [string, () => Response][]) {
  const fetchMock = vi.fn((input: string | URL | Request) => {
    const url = typeof input === 'string' ? input : input.toString()
    const match = routes.find(([prefix]) => url.startsWith(prefix))
    if (!match) {
      return Promise.reject(new Error(`no route for ${url}`))
    }
    const response = match[1]()
    if (!response.url) {
      Object.defineProperty(response, 'url', { value: url })
    }
    return Promise.resolve(response)
  })
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

function throwingFetch() {
  const fetchMock = vi.fn(() => Promise.reject(new Error('network error')))
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

const html = () =>
  new Response('<html></html>', { status: 200, headers: { 'content-type': 'text/html' } })
const htmlAt = (finalUrl: string) => {
  const response = html()
  Object.defineProperty(response, 'url', { value: finalUrl })
  return response
}
const notFound = () => new Response('not found', { status: 404 })

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('llmsTxtProbe', () => {
  it('accepts a long, non-html llms.txt body', async () => {
    routeFetch([
      ['https://docs.example.com/llms.txt', () => new Response('x'.repeat(60), { status: 200 })],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.example.com',
        method: 'llms-txt-probe',
        confidence: 'high',
        livenessOk: true,
      },
    ])
  })

  it('rejects an html body', async () => {
    routeFetch([
      [
        'https://docs.example.com/llms.txt',
        () => new Response('<html>' + 'x'.repeat(60) + '</html>'),
      ],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([])
  })

  it('rejects a short body', async () => {
    routeFetch([['https://docs.example.com/llms.txt', () => new Response('too short')]])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([])
  })

  it('returns [] when website is missing', async () => {
    expect(
      await llmsTxtProbe({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] when the website does not resolve to a domain', async () => {
    expect(
      await llmsTxtProbe({
        name: 'proj',
        slug: 'proj',
        website: '::not a url::',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('falls back to the root domain when the docs subdomain has no llms.txt', async () => {
    routeFetch([
      ['https://docs.example.com/llms.txt', notFound],
      ['https://example.com/llms.txt', () => new Response('x'.repeat(60), { status: 200 })],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://example.com',
        method: 'llms-txt-probe',
        confidence: 'high',
        livenessOk: true,
      },
    ])
  })

  it('falls back to the root domain when the docs subdomain serves an html shell instead of a 404', async () => {
    routeFetch([
      [
        'https://docs.example.com/llms.txt',
        () => new Response('<html>' + 'x'.repeat(60), { status: 200 }),
      ],
      ['https://example.com/llms.txt', () => new Response('x'.repeat(60), { status: 200 })],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://example.com',
        method: 'llms-txt-probe',
        confidence: 'high',
        livenessOk: true,
      },
    ])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await llmsTxtProbe({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('llmsTxtProbe website path and gating', () => {
  const llms = () => new Response('x'.repeat(60), { status: 200 })

  it('probes the website path before the root and returns the base that served the file', async () => {
    const fetchMock = routeFetch([
      ['https://foundation.org/projects/x/llms.txt', llms],
      ['https://foundation.org/llms.txt', llms],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://foundation.org/projects/x/',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://foundation.org/projects/x',
        method: 'llms-txt-probe',
        confidence: 'high',
        livenessOk: true,
      },
    ])
    expect(fetchMock.mock.calls[0][0]).toBe('https://foundation.org/projects/x/llms.txt')
  })

  it('falls through to docs subdomain then the bare root when the path has no llms.txt', async () => {
    routeFetch([
      ['https://foundation.org/projects/x/llms.txt', notFound],
      ['https://docs.foundation.org/llms.txt', notFound],
      ['https://foundation.org/llms.txt', llms],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://foundation.org/projects/x',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result.map((r) => r.url)).toEqual(['https://foundation.org'])
  })

  it('does not probe the root twice for a root website', async () => {
    const fetchMock = routeFetch([
      ['https://docs.example.com/llms.txt', notFound],
      ['https://example.com/llms.txt', notFound],
    ])

    await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com/',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      'https://docs.example.com/llms.txt',
      'https://example.com/llms.txt',
    ])
  })

  it('returns [] for a github website without fetching', async () => {
    const fetchMock = routeFetch([['https://github.com', llms]])

    expect(
      await llmsTxtProbe({
        name: 'proj',
        slug: 'proj',
        website: 'https://github.com/org/repo',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
    expect(
      await llmsTxtProbe({
        name: 'proj',
        slug: 'proj',
        website: 'https://www.github.com/org/repo',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('websiteShared gating', () => {
  it('makes every website-anchored strategy return [] without fetching', async () => {
    const fetchMock = routeFetch([['https://', html]])
    const shared = {
      name: 'proj',
      slug: 'proj',
      website: 'https://foundation.org/projects/x',
      websiteShared: true,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    }

    expect(await llmsTxtProbe(shared)).toEqual([])
    expect(await docsSubdomain(shared)).toEqual([])
    expect(await docsPath(shared)).toEqual([])
    expect(await projectWebsite(shared)).toEqual([])
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('skips projectWebsite, docsSubdomain and docsPath for a github website', async () => {
    const fetchMock = routeFetch([['https://', html]])
    const gh = {
      name: 'proj',
      slug: 'proj',
      website: 'https://github.com/org/repo',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    }

    expect(await docsSubdomain(gh)).toEqual([])
    expect(await docsPath(gh)).toEqual([])
    expect(await projectWebsite(gh)).toEqual([])
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('githubHomepage derived probes', () => {
  const llms = () => new Response('x'.repeat(60), { status: 200 })
  const ctx = (over = {}) => ({
    name: 'proj',
    slug: 'proj',
    website: 'https://foundation.org/projects/x',
    websiteShared: true,
    repos,
    githubToken: 'token',
    serpApiKey: null,
    ...over,
  })

  it('probes the repo homepage domain when the website is shared, calling the API once', async () => {
    const fetchMock = routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://www.openvdb.org' }),
      ],
      ['https://www.openvdb.org/documentation', html],
      ['https://www.openvdb.org/llms.txt', notFound],
      ['https://www.openvdb.org/__docs-readiness', notFound],
      ['https://www.openvdb.org', html],
    ])

    const result = await githubHomepage(ctx())

    expect(result.map((c) => [c.method, c.url])).toEqual(
      expect.arrayContaining([
        ['github-homepage', 'https://www.openvdb.org/'],
        ['docs-path', 'https://www.openvdb.org/documentation'],
      ]),
    )
    expect(
      fetchMock.mock.calls.filter((call) => new URL(String(call[0])).hostname === 'api.github.com'),
    ).toHaveLength(1)
    expect(result.some((c) => new URL(c.url).hostname === 'foundation.org')).toBe(false)
  })

  it('derives an llms-txt-probe candidate from the homepage domain', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://openvdb.org' }),
      ],
      ['https://openvdb.org/llms.txt', llms],
      ['https://', notFound],
    ])

    const result = await githubHomepage(ctx({ website: null, websiteShared: false }))
    expect(result).toContainEqual({
      url: 'https://openvdb.org',
      method: 'llms-txt-probe',
      confidence: 'high',
      livenessOk: true,
    })
  })

  it.each([
    'https://www.npmjs.com/package/foo',
    'https://pypi.org/project/foo',
    'https://lfenergy.org/projects/x',
  ])(
    'probes only paths under the homepage %s, never its docs subdomain or root',
    async (homepage) => {
      const fetchMock = routeFetch([
        ['https://api.github.com/repos/torvalds/linux', () => Response.json({ homepage })],
        ['https://', notFound],
      ])

      await githubHomepage(ctx())

      const probed = fetchMock.mock.calls
        .map((call) => String(call[0]))
        .filter((url) => new URL(url).hostname !== 'api.github.com')
      expect(probed.filter((url) => new URL(url).hostname.startsWith('docs.'))).toEqual([])
      expect(probed.filter((url) => new URL(url).pathname === '/llms.txt')).toEqual([])
      expect(probed.sort()).toEqual(
        [
          homepage,
          `${homepage}/doc`,
          `${homepage}/docs`,
          `${homepage}/documentation`,
          `${homepage}/llms.txt`,
        ].sort(),
      )
    },
  )

  it('still finds an llms.txt served under the homepage path', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://lfenergy.org/projects/x' }),
      ],
      ['https://lfenergy.org/projects/x/llms.txt', llms],
      ['https://', notFound],
    ])

    expect(await githubHomepage(ctx())).toContainEqual({
      url: 'https://lfenergy.org/projects/x',
      method: 'llms-txt-probe',
      confidence: 'high',
      livenessOk: true,
    })
  })

  it('still probes the docs subdomain and root llms.txt for a root-path homepage', async () => {
    const fetchMock = routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://openvdb.org' }),
      ],
      ['https://', notFound],
    ])

    await githubHomepage(ctx())

    const probed = fetchMock.mock.calls.map((call) => String(call[0]))
    expect(probed).toEqual(
      expect.arrayContaining([
        'https://docs.openvdb.org',
        'https://docs.openvdb.org/llms.txt',
        'https://openvdb.org/llms.txt',
      ]),
    )
  })

  it('does no derived probing when the homepage is on the shared website host', async () => {
    const fetchMock = routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://foundation.org/projects/x/' }),
      ],
      ['https://foundation.org/projects/x', html],
    ])

    const result = await githubHomepage(ctx())
    expect(result.map((c) => c.method)).toEqual(['github-homepage'])
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('does not re-probe a homepage that equals the usable website', async () => {
    const fetchMock = routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://example.com' }),
      ],
      ['https://example.com', html],
    ])

    await githubHomepage(ctx({ website: 'https://www.example.com', websiteShared: false }))
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('does not derive docs.docs.<domain> when the homepage is already a docs host', async () => {
    const fetchMock = routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://docs.example.com' }),
      ],
      ['https://docs.example.com', html],
    ])

    await githubHomepage(ctx({ website: null, websiteShared: false }))
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('returns [] from website strategies when shared with no website', async () => {
    const fetchMock = routeFetch([['https://', html]])
    const noSite = ctx({ website: null })

    expect(await llmsTxtProbe(noSite)).toEqual([])
    expect(await projectWebsite(noSite)).toEqual([])
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('does no derived probing when the homepage is a github url', async () => {
    const fetchMock = routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://github.com/torvalds' }),
      ],
    ])

    expect(await githubHomepage(ctx())).toEqual([])
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})

describe('discoverDocs with a shared website', () => {
  it('resolves to the docs path derived from the repo homepage', async () => {
    routeFetch([
      ['https://api.github.com/repos/torvalds/linux/contents', notFound],
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://www.openvdb.org' }),
      ],
      ['https://www.openvdb.org/documentation', html],
      ['https://www.openvdb.org/docs', notFound],
      ['https://www.openvdb.org/doc', notFound],
      ['https://www.openvdb.org/__docs-readiness', notFound],
      ['https://www.openvdb.org', html],
    ])

    const result = await discoverDocs({
      name: 'OpenVDB',
      slug: 'openvdb',
      website: 'https://foundation.org/projects/openvdb',
      websiteShared: true,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })

    expect(result.docsUrl).toBe('https://www.openvdb.org/documentation')
    expect(result.allCandidates.some((c) => new URL(c.url).hostname === 'foundation.org')).toBe(
      false,
    )
  })
})

describe('llmsTxtProbe registrable domain', () => {
  it('probes docs.<registrable domain>/llms.txt for a subdomain website', async () => {
    const fetchMock = routeFetch([
      ['https://docs.opendaylight.org/llms.txt', () => new Response('x'.repeat(60))],
    ])

    const result = await llmsTxtProbe({
      name: 'proj',
      slug: 'proj',
      website: 'https://wiki.opendaylight.org/view/Main',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result.map((c) => c.url)).toEqual(['https://docs.opendaylight.org'])
    expect(fetchMock.mock.calls.map(([u]) => String(u))).toEqual([
      'https://wiki.opendaylight.org/view/Main/llms.txt',
      'https://docs.opendaylight.org/llms.txt',
    ])
  })

  it('falls back to the website own host, not the registrable root', async () => {
    const fetchMock = routeFetch([['https://', notFound]])

    await llmsTxtProbe({
      name: 'infiniedge',
      slug: 'infiniedge',
      website: 'https://infiniedge.lfedge.org',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(fetchMock.mock.calls.map(([u]) => String(u))).toEqual([
      'https://docs.infiniedge.lfedge.org/llms.txt',
      'https://infiniedge.lfedge.org/llms.txt',
    ])
  })

  it('ignores an llms.txt served after a cross-domain redirect', async () => {
    routeFetch([
      [
        'https://docs.example.com/llms.txt',
        () => {
          const response = new Response('x'.repeat(60))
          Object.defineProperty(response, 'url', { value: 'https://spam-casino.net/llms.txt' })
          return response
        },
      ],
    ])

    expect(
      await llmsTxtProbe({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('docsSubdomain', () => {
  it('returns a candidate when the docs subdomain is live', async () => {
    routeFetch([['https://docs.example.com', html]])

    const result = await docsSubdomain({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.example.com',
        method: 'docs-subdomain',
        confidence: 'high',
        livenessOk: true,
      },
    ])
  })

  it('returns [] when not live', async () => {
    routeFetch([['https://docs.example.com', notFound]])
    expect(
      await docsSubdomain({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] when website is missing', async () => {
    expect(
      await docsSubdomain({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('probes the real domain, not "docs.null", for a scheme-less website', async () => {
    routeFetch([['https://docs.example.com', html]])

    const result = await docsSubdomain({
      name: 'proj',
      slug: 'proj',
      website: 'example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.example.com',
        method: 'docs-subdomain',
        confidence: 'high',
        livenessOk: true,
      },
    ])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await docsSubdomain({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('docsSubdomain registrable domain', () => {
  it('probes docs.<registrable domain> for a subdomain website', async () => {
    const fetchMock = routeFetch([['https://docs.opendaylight.org', html]])

    const result = await docsSubdomain({
      name: 'proj',
      slug: 'proj',
      website: 'https://wiki.opendaylight.org/view/Main',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.opendaylight.org',
        method: 'docs-subdomain',
        confidence: 'high',
        livenessOk: true,
      },
    ])
    expect(fetchMock.mock.calls.map(([u]) => String(u))).toEqual(['https://docs.opendaylight.org'])
  })

  it('keeps the full host on the linuxfoundation.org umbrella root (celf)', async () => {
    const fetchMock = routeFetch([['https://docs.wiki.linuxfoundation.org', html]])

    await docsSubdomain({
      name: 'celf',
      slug: 'celf',
      website: 'https://wiki.linuxfoundation.org/celp/start',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(fetchMock.mock.calls.map(([u]) => String(u))).toEqual([
      'https://docs.wiki.linuxfoundation.org',
    ])
  })

  it('keeps the full host for a project-specific subdomain', async () => {
    const fetchMock = routeFetch([['https://docs.developers.google.com', html]])

    await docsSubdomain({
      name: 'tink',
      slug: 'tink',
      website: 'https://developers.google.com/tink',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(fetchMock.mock.calls.map(([u]) => String(u))).toEqual([
      'https://docs.developers.google.com',
    ])
  })

  it('returns [] when docs.<domain> redirects to another registrable domain', async () => {
    routeFetch([['https://docs.example.com', () => htmlAt('https://spam-casino.net/')]])

    expect(
      await docsSubdomain({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('docsPath', () => {
  it('returns a candidate for each live path', async () => {
    routeFetch([
      ['https://example.com/docs', html],
      ['https://example.com/documentation', notFound],
      ['https://example.com/doc', html],
    ])

    const result = await docsPath({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://example.com/docs',
        method: 'docs-path',
        confidence: 'medium',
        livenessOk: true,
      },
      {
        url: 'https://example.com/doc',
        method: 'docs-path',
        confidence: 'medium',
        livenessOk: true,
      },
    ])
  })

  it('returns [] when the site answers 200 for every path (soft 404)', async () => {
    routeFetch([['https://example.com', html]])

    expect(
      await docsPath({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('keeps a real /docs under a website path when unknown paths return 404', async () => {
    const fetchMock = routeFetch([
      ['https://foundation.org/projects/x/__docs-readiness', notFound],
      ['https://foundation.org/projects/x/docs', html],
      ['https://foundation.org/projects/x/doc', notFound],
    ])

    const result = await docsPath({
      name: 'proj',
      slug: 'proj',
      website: 'https://foundation.org/projects/x/',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result.map((c) => c.url)).toEqual(['https://foundation.org/projects/x/docs'])
    expect(fetchMock.mock.calls.map(([input]) => input.toString())).toContain(
      'https://foundation.org/projects/x/__docs-readiness-soft-404-probe__',
    )
  })

  it('probes the path instead of appending after a query string', async () => {
    const fetchMock = routeFetch([
      ['https://example.com/docs', html],
      ['https://example.com/documentation', notFound],
      ['https://example.com/doc', notFound],
    ])

    const result = await docsPath({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com?ref=x',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://example.com/docs',
        method: 'docs-path',
        confidence: 'medium',
        livenessOk: true,
      },
    ])
    expect(fetchMock.mock.calls.map(([input]) => input.toString())).toContain(
      'https://example.com/docs',
    )
  })

  it('returns [] when website is missing', async () => {
    expect(
      await docsPath({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await docsPath({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

const repos = [{ url: 'https://github.com/torvalds/linux', starCount: null }]

describe('packageManifest', () => {
  it('returns a candidate from the package.json documentation field', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux/contents/package.json',
        () => new Response(JSON.stringify({ documentation: 'https://docs.example.com' })),
      ],
      ['https://docs.example.com', html],
    ])

    const result = await packageManifest({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.example.com/',
        method: 'package-manifest',
        confidence: 'medium',
        livenessOk: true,
      },
    ])
  })

  it('returns [] when there is no github repo', async () => {
    expect(
      await packageManifest({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] when there is no github token', async () => {
    expect(
      await packageManifest({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await packageManifest({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('readmeScrape', () => {
  it('returns a candidate for a live docs link mentioned in the readme', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux/readme',
        () => new Response('See the [Documentation](https://docs.example.com) for more.'),
      ],
      ['https://docs.example.com', html],
    ])

    const result = await readmeScrape({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.example.com',
        method: 'readme-scrape',
        confidence: 'medium',
        livenessOk: true,
      },
    ])
  })

  it('returns [] when there is no github repo', async () => {
    expect(
      await readmeScrape({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] when there is no github token', async () => {
    expect(
      await readmeScrape({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await readmeScrape({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('readmeScrape filtering', () => {
  const readmeRoute = (body: string): [string, () => Response] => [
    'https://api.github.com/repos/torvalds/linux/readme',
    () => new Response(body),
  ]
  const probed = (fetchMock: ReturnType<typeof routeFetch>) =>
    fetchMock.mock.calls
      .map(([u]) => String(u))
      .filter((u) => new URL(u).hostname !== 'api.github.com')
  const scrape = (website: string | null) =>
    readmeScrape({
      name: 'proj',
      slug: 'proj',
      website,
      websiteShared: false,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })
  const scrapeSharedWebsite = (website: string) =>
    readmeScrape({
      name: 'proj',
      slug: 'proj',
      website,
      websiteShared: true,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })
  const homepageRoute = (homepage: string): [string, () => Response] => [
    'https://api.github.com/repos/torvalds/linux',
    () => Response.json({ homepage }),
  ]

  it('never proposes docs.github.com or other excluded hosts', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        [
          '[Docs](https://docs.github.com/en/actions)',
          '[Docs](https://twitter.com/proj/docs)',
          '[Docs](https://www.youtube.com/watch?v=docs)',
          '[Docs](https://discord.gg/docs)',
        ].join(' '),
      ),
    ])

    expect(await scrape(null)).toEqual([])
    expect(probed(fetchMock)).toEqual([])
  })

  it('drops contributing/issues/changelog/license/security style paths', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        [
          '[Docs](https://urunc.io/docs/contributing/)',
          '[Guide](https://urunc.io/developer-guide/contribute/#reporting-bugs)',
          '[Docs](https://urunc.io/docs/changelog)',
          '[Docs](https://urunc.io/docs/LICENSE)',
          '[Docs](https://urunc.io/docs/issues)',
          '[Guide](https://other.io/guide/code-of-conduct)',
          '[Security Policy document](https://urunc.io/developer-guide/security/)',
        ].join(' '),
      ),
    ])

    expect(await scrape('https://urunc.io')).toEqual([])
    expect(probed(fetchMock)).toEqual([])
  })

  it('excludes by whole slug: security-model is dropped, issuers and bugzilla are kept', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://a.io/docs/security-model) [Docs](https://b.io/docs/issuers) [Docs](https://c.io/docs/bugzilla)',
      ),
      ['https://', html],
    ])

    await scrape(null)
    expect(probed(fetchMock)).toEqual(['https://b.io/docs/issuers', 'https://c.io/docs/bugzilla'])
  })

  it('keeps a path whose segment merely contains an excluded word', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://example.io/docs/debugging) [Docs](https://example.io/docs/security)',
      ),
      ['https://example.io', html],
    ])

    await scrape(null)
    expect(probed(fetchMock)).toEqual(['https://example.io/docs/debugging'])
  })

  it('keeps the docs-labelled text when a fragment variant of the link came first', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Website](https://proj.io/) [Documentation](https://proj.io#docs)'),
      ['https://proj.io', html],
    ])

    await scrape(null)
    expect(probed(fetchMock)).toEqual(['https://proj.io'])
  })

  it('collapses to the path through the first docs segment', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Docs](https://proj.io/en/docs/install/linux)'),
      ['https://proj.io', html],
    ])

    await scrape('https://proj.io')
    expect(probed(fetchMock)).toEqual(['https://proj.io/en/docs'])
  })

  it('does not collapse links when the project website is itself a github.io page', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Docs](https://torvalds.github.io/linux/guide/install)'),
      ['https://torvalds.github.io', html],
    ])

    await scrape('https://torvalds.github.io/linux')
    expect(probed(fetchMock)).toEqual(['https://torvalds.github.io/linux/guide/install'])
  })

  it('collapses own-domain deep links and dedupes fragment variants', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        [
          '[Docs](https://urunc.io/docs/install/linux/#step-2)',
          '[Docs](https://urunc.io/docs/install/macos)',
          '[Guide](https://urunc.io/developer-guide/)',
          '[Guide](https://urunc.io/developer-guide#intro)',
          '[Reference](https://urunc.io/reference/cli/flags)',
        ].join(' '),
      ),
      ['https://urunc.io', html],
    ])

    const result = await scrape('https://urunc.io')
    expect(probed(fetchMock)).toEqual([
      'https://urunc.io/docs',
      'https://urunc.io/developer-guide/',
      'https://urunc.io/reference',
    ])
    expect(result.map((c) => c.url)).toEqual([
      'https://urunc.io/docs',
      'https://urunc.io/developer-guide/',
      'https://urunc.io/reference',
    ])
  })

  it('treats sibling sites on a shared registrable domain as foreign, not own', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Governance docs](https://community.finos.org/docs/governance/) [Guide](https://odp.finos.org/docs/a/b)',
      ),
      ['https://odp.finos.org', html],
      ['https://community.finos.org', html],
    ])

    await scrape('https://odp.finos.org')
    expect(probed(fetchMock)).toEqual(['https://odp.finos.org/docs'])
  })

  it('dedupes http, https and www variants of the same link', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://proj.io/docs) [Docs](https://www.proj.io/docs) [Docs](http://proj.io/docs)',
      ),
      ['http', html],
    ])

    await scrape('https://proj.io')
    expect(probed(fetchMock)).toEqual(['https://proj.io/docs'])
  })

  it('collapses a deep own-domain link with no docs segment to the site root', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Docs](https://urunc.io/learn/getting-started/install)'),
      ['https://urunc.io', html],
    ])

    await scrape('https://urunc.io')
    expect(probed(fetchMock)).toEqual(['https://urunc.io/'])
  })

  it('keeps foreign links when no own-domain link survives', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[OpenLineage docs](https://openlineage.io/docs/)'),
      ['https://openlineage.io', html],
    ])

    const result = await scrape('https://marquezproject.ai')
    expect(probed(fetchMock)).toEqual(['https://openlineage.io/docs/'])
    expect(result).toHaveLength(1)
  })

  it('drops foreign links once an own-domain link survives', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[docker](https://docs.docker.com/engine/install/ubuntu/) [Guide](https://urunc.io/developer-guide/)',
      ),
      ['https://urunc.io', html],
      ['https://docs.docker.com', html],
    ])

    await scrape('https://urunc.io')
    expect(probed(fetchMock)).toEqual(['https://urunc.io/developer-guide/'])
  })

  it('treats the repo homepage domain as the project own domain when website is empty', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Website](https://proj.io/docs/a) [Docs](https://elsewhere.net/x) [Ref](https://proj.io/reference/cli/y)',
      ),
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://proj.io' }),
      ],
      ['https://proj.io', html],
    ])

    await scrape('')
    expect(probed(fetchMock)).toEqual(['https://proj.io/docs', 'https://proj.io/reference'])
  })

  it('does not treat a root homepage on the shared website host as an own domain', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://www.lfedge.org/projects/other/docs/guide/page) [Guide](https://docs.real-project.dev/start)',
      ),
      homepageRoute('https://www.lfedge.org'),
      ['https://', html],
    ])

    await scrapeSharedWebsite('https://www.lfedge.org/projects/x')
    expect(probed(fetchMock)).toEqual([
      'https://www.lfedge.org/projects/other/docs/guide/page',
      'https://docs.real-project.dev/start',
    ])
  })

  it.each([
    ['https://www.npmjs.com/package/x', 'https://www.npmjs.com/package/x/docs/api'],
    ['https://pypi.org/project/x', 'https://pypi.org/project/x/docs/api'],
    ['https://lfenergy.org/projects/x', 'https://lfenergy.org/projects/other/docs/api'],
    ['https://pypi.org', 'https://pypi.org/project/x/docs/api'],
    ['https://crates.io', 'https://crates.io/crates/x/docs/api'],
    ['https://hub.docker.com', 'https://hub.docker.com/r/x/docs/api'],
  ])('does not make the homepage %s an own domain', async (homepage, link) => {
    const fetchMock = routeFetch([
      readmeRoute(`[Docs](${link}) [Guide](https://docs.real-project.dev/start)`),
      homepageRoute(homepage),
      ['https://', html],
    ])

    await scrape(null)
    expect(probed(fetchMock)).toEqual([link, 'https://docs.real-project.dev/start'])
  })

  it('keeps a root homepage on a host other than the shared website as an own domain', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://openvdb.org/docs/guide/page) [Guide](https://elsewhere.net/docs/x)',
      ),
      homepageRoute('https://openvdb.org'),
      ['https://', html],
    ])

    await scrapeSharedWebsite('https://foundation.org/projects/x')
    expect(probed(fetchMock)).toEqual(['https://openvdb.org/docs'])
  })

  it('falls back to the original deep link when the collapsed one is not live', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Docs](https://jestjs.io/docs/using-matchers)'),
      ['https://jestjs.io/docs/using-matchers', html],
      ['https://jestjs.io/docs', notFound],
    ])

    const result = await scrape('https://jestjs.io')
    expect(probed(fetchMock)).toEqual([
      'https://jestjs.io/docs',
      'https://jestjs.io/docs/using-matchers',
    ])
    expect(result.map((c) => c.url)).toEqual(['https://jestjs.io/docs/using-matchers'])
  })

  it('keeps the deep-link fallback when the docs prefix link came first', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://jestjs.io/docs) [Matchers](https://jestjs.io/docs/using-matchers)',
      ),
      ['https://jestjs.io/docs/using-matchers', html],
      ['https://jestjs.io/docs', notFound],
    ])

    const result = await scrape('https://jestjs.io')
    expect(probed(fetchMock)).toEqual([
      'https://jestjs.io/docs',
      'https://jestjs.io/docs/using-matchers',
    ])
    expect(result.map((c) => c.url)).toEqual(['https://jestjs.io/docs/using-matchers'])
  })

  it('orders own website before <owner>.github.io and probes at most 5 links', async () => {
    const links = [
      '[Docs](https://torvalds.github.io/linux/docs)',
      '[Docs](https://proj.dev/docs)',
      '[Guide](https://proj.dev/guide)',
      '[Manual](https://proj.dev/manual)',
      '[Reference](https://proj.dev/reference)',
      '[Handbook](https://proj.dev/handbook)',
      '[Docs](https://proj.dev/documentation)',
    ]
    const fetchMock = routeFetch([readmeRoute(links.join(' ')), ['https://', notFound]])

    await scrape('https://proj.dev')
    expect(probed(fetchMock)).toEqual([
      'https://proj.dev/docs',
      'https://proj.dev/guide',
      'https://proj.dev/manual',
      'https://proj.dev/reference',
      'https://proj.dev/handbook',
    ])
  })

  it('probes <owner>.github.io before foreign links and skips foreign ones once it is live', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://a.example.net/docs) [Docs](https://torvalds.github.io/linux/docs)',
      ),
      ['https://torvalds.github.io', html],
    ])

    await scrape('https://proj.dev')
    expect(probed(fetchMock)).toEqual(['https://torvalds.github.io/linux/docs'])
  })

  it('falls back to foreign links when no own-domain link is live', async () => {
    const fetchMock = routeFetch([
      readmeRoute(
        '[Docs](https://proj.io/docs/old/x) [Docs](https://proj.readthedocs.io/en/latest/)',
      ),
      ['https://proj.readthedocs.io', html],
      ['https://proj.io', notFound],
    ])

    const result = await scrape('https://proj.io')
    expect(probed(fetchMock)).toEqual([
      'https://proj.io/docs',
      'https://proj.io/docs/old/x',
      'https://proj.readthedocs.io/en/latest/',
    ])
    expect(result.map((c) => c.url)).toEqual(['https://proj.readthedocs.io/en/latest/'])
  })

  it('counts fallback re-probes against the 5-probe budget', async () => {
    const links = ['en', 'de', 'fr', 'es'].map((x) => `[Docs](https://proj.io/${x}/docs/deep)`)
    const fetchMock = routeFetch([readmeRoute(links.join(' ')), ['https://', notFound]])

    await scrape('https://proj.io')
    expect(probed(fetchMock)).toHaveLength(5)
  })

  it('collapses a single-segment own-domain page without a docs word to the root', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Docs](https://urunc.io/installation/)'),
      ['https://urunc.io', html],
    ])

    await scrape('https://urunc.io')
    expect(probed(fetchMock)).toEqual(['https://urunc.io/'])
  })

  it('does not collapse <owner>.github.io project pages to the site root', async () => {
    const fetchMock = routeFetch([
      readmeRoute('[Docs](https://torvalds.github.io/linux/guide/install)'),
      ['https://torvalds.github.io', html],
    ])

    await scrape(null)
    expect(probed(fetchMock)).toEqual(['https://torvalds.github.io/linux/guide/install'])
  })

  it('rejects a readme link that redirects to another registrable domain', async () => {
    routeFetch([
      readmeRoute('[Docs](https://zotregistry.io/docs)'),
      ['https://zotregistry.io', () => htmlAt('https://spam-casino.net/')],
    ])

    expect(await scrape('https://zotregistry.io')).toEqual([])
  })
})

describe('readmeScrape foreign links', () => {
  const scrape = () =>
    readmeScrape({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })
  const readme = (body: string): [string, () => Response] => [
    'https://api.github.com/repos/torvalds/linux/readme',
    () => new Response(body),
  ]

  const apiFree = (fetchMock: ReturnType<typeof routeFetch>) =>
    fetchMock.mock.calls
      .map(([u]) => String(u))
      .filter((u) => new URL(u).hostname !== 'api.github.com')

  it('drops a foreign link matched only by a docs-like path (RFC under /doc/)', async () => {
    const fetchMock = routeFetch([
      readme('[JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7396)'),
      ['https://datatracker.ietf.org', html],
    ])

    expect(await scrape()).toEqual([])
    expect(apiFree(fetchMock)).toEqual([])
  })

  it('keeps a foreign link whose text or host says docs', async () => {
    const fetchMock = routeFetch([
      readme('[OpenLineage docs](https://openlineage.io/x) [Ext](https://docs.docker.com/engine/)'),
      ['https://openlineage.io', html],
      ['https://docs.docker.com', html],
    ])

    await scrape()
    expect(apiFree(fetchMock)).toEqual([
      'https://openlineage.io/x',
      'https://docs.docker.com/engine/',
    ])
  })

  it('never proposes google docs documents', async () => {
    const fetchMock = routeFetch([
      readme(
        '[Meeting notes](https://docs.google.com/document/d/abc123) [Docs](https://drive.google.com/x)',
      ),
    ])

    expect(await scrape()).toEqual([])
    expect(apiFree(fetchMock)).toEqual([])
  })

  it('never proposes github-owned asset hosts', async () => {
    const fetchMock = routeFetch([
      readme('[Docs](https://raw.githubusercontent.com/o/r/main/docs/a.md)'),
    ])

    expect(await scrape()).toEqual([])
    expect(apiFree(fetchMock)).toEqual([])
  })
})

describe('readmeScrape and llms coverage gaps', () => {
  const readmeRoute = (body: string): [string, () => Response] => [
    'https://api.github.com/repos/torvalds/linux/readme',
    () => new Response(body),
  ]
  const probed = (m: ReturnType<typeof routeFetch>) =>
    m.mock.calls.map(([u]) => String(u)).filter((u) => new URL(u).hostname !== 'api.github.com')
  const scrape = (website: string | null) =>
    readmeScrape({
      name: 'p',
      slug: 'p',
      website,
      websiteShared: false,
      repos,
      githubToken: 't',
      serpApiKey: null,
    })

  it('llms fallback rejects a cross-domain redirect', async () => {
    routeFetch([
      ['https://docs.example.com/llms.txt', notFound],
      [
        'https://example.com/llms.txt',
        () => {
          const r = new Response('x'.repeat(60))
          Object.defineProperty(r, 'url', { value: 'https://spam.net/llms.txt' })
          return r
        },
      ],
    ])
    expect(
      await llmsTxtProbe({
        name: 'p',
        slug: 'p',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
  it('llms fallback probes the website own host for a generic-subdomain website', async () => {
    const m = routeFetch([['https://', notFound]])
    await llmsTxtProbe({
      name: 'p',
      slug: 'p',
      website: 'https://wiki.example.org/x',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(m.mock.calls.map(([u]) => String(u))).toEqual([
      'https://wiki.example.org/x/llms.txt',
      'https://docs.example.org/llms.txt',
      'https://wiki.example.org/llms.txt',
    ])
  })
  it('drops bug / report / reporting-bugs paths', async () => {
    const m = routeFetch([
      readmeRoute(
        '[Docs](https://a.io/docs/bugs) [Docs](https://b.io/docs/report) [Docs](https://c.io/docs/reporting-bugs)',
      ),
      ['https://', html],
    ])
    await scrape(null)
    expect(probed(m)).toEqual([])
  })
  it('x.com exclusion is dot-bounded (box.com / dropbox.com stay)', async () => {
    const m = routeFetch([
      readmeRoute(
        '[Docs](https://docs.box.com/guide) [Docs](https://docs.dropbox.com/documentation)',
      ),
      ['https://', html],
    ])
    await scrape(null)
    expect(probed(m)).toEqual([
      'https://docs.box.com/guide',
      'https://docs.dropbox.com/documentation',
    ])
  })
  it('excluded hosts gitter/x/slack/youtu.be/githubassets/shields', async () => {
    const m = routeFetch([
      readmeRoute(
        '[Docs](https://gitter.im/docs) [Docs](https://x.com/docs) [Docs](https://slack.com/docs) [Docs](https://youtu.be/docs) [Docs](https://a.githubassets.com/docs) [Docs](https://img.shields.io/badge/docs-passing-green.svg)',
      ),
      ['https://', html],
    ])
    await scrape(null)
    expect(probed(m)).toEqual([])
  })
  it('own link on a subdomain of the base is own (collapsed) and gates foreign', async () => {
    const m = routeFetch([
      readmeRoute('[Docs](https://docs.urunc.io/en/latest/x/y) [Docs](https://other.net/docs)'),
      ['https://', html],
    ])
    await scrape('https://urunc.io')
    expect(probed(m)).toEqual(['https://docs.urunc.io/'])
  })
  it('generic-subdomain website: docs.<root> is own', async () => {
    const m = routeFetch([
      readmeRoute(
        '[Docs](https://docs.example.org/en/latest/getting-started-guide/intro) [Docs](https://other.net/docs)',
      ),
      ['https://', html],
    ])
    await scrape('https://wiki.example.org/x')
    expect(probed(m)).toEqual(['https://docs.example.org/en/latest/getting-started-guide'])
  })
  it('own link without docs keyword is dropped', async () => {
    const m = routeFetch([
      readmeRoute('[Blog](https://proj.io/blog/post) [About](https://proj.io/about)'),
      ['https://', html],
    ])
    await scrape('https://proj.io')
    expect(probed(m)).toEqual([])
  })
  it('dot / underscore boundaries and licence spelling', async () => {
    const m = routeFetch([
      readmeRoute(
        '[Docs](https://a.io/docs/CHANGELOG.md) [Docs](https://a.io/docs/security.html) [Docs](https://a.io/docs/security_policy) [Docs](https://a.io/docs/licence) [Docs](https://a.io/docs/code_of_conduct)',
      ),
      ['https://', html],
    ])
    await scrape(null)
    expect(probed(m)).toEqual([])
  })
  it('dedupe treats trailing-slash variants as one', async () => {
    const m = routeFetch([
      readmeRoute('[Guide](https://proj.io/guide/) [Guide](https://proj.io/guide/install/x)'),
      ['https://', html],
    ])
    await scrape('https://proj.io')
    expect(probed(m)).toEqual(['https://proj.io/guide/'])
  })
  it('collapse drops the query string', async () => {
    const m = routeFetch([
      readmeRoute('[Docs](https://proj.io/docs/install/linux?ref=x)'),
      ['https://', html],
    ])
    await scrape('https://proj.io')
    expect(probed(m)).toEqual(['https://proj.io/docs'])
  })
  it('collapse keeps up to the FIRST docs segment', async () => {
    const m = routeFetch([
      readmeRoute('[Docs](https://proj.io/docs/guide/install)'),
      ['https://', html],
    ])
    await scrape('https://proj.io')
    expect(probed(m)).toEqual(['https://proj.io/docs'])
  })
})

describe('githubHomepage', () => {
  it('returns a candidate from the repo homepage field', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://docs.example.com' }),
      ],
      ['https://docs.example.com/doc', notFound],
      ['https://docs.example.com', html],
    ])

    const result = await githubHomepage({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos,
      githubToken: 'token',
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://docs.example.com/',
        method: 'github-homepage',
        confidence: 'medium',
        livenessOk: true,
      },
    ])
  })

  it('skips a homepage pointing back at the repo itself', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://github.com/torvalds/linux' }),
      ],
    ])

    expect(
      await githubHomepage({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('skips a homepage pointing at www.github.com', async () => {
    routeFetch([
      [
        'https://api.github.com/repos/torvalds/linux',
        () => Response.json({ homepage: 'https://www.github.com/torvalds/linux' }),
      ],
    ])

    expect(
      await githubHomepage({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] when there is no github repo', async () => {
    expect(
      await githubHomepage({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] when there is no github token', async () => {
    expect(
      await githubHomepage({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await githubHomepage({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos,
        githubToken: 'token',
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('projectWebsite cross-domain redirect', () => {
  it('drops a lapsed website that redirects to an unrelated domain (zot)', async () => {
    routeFetch([['https://zotregistry.io', () => htmlAt('https://spam-casino.net/')]])

    expect(
      await projectWebsite({
        name: 'zot',
        slug: 'zot',
        website: 'https://zotregistry.io',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('projectWebsite', () => {
  it('returns a candidate when the website is live', async () => {
    routeFetch([['https://example.com', html]])

    const result = await projectWebsite({
      name: 'proj',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([
      {
        url: 'https://example.com/',
        method: 'project-website',
        confidence: 'low',
        livenessOk: true,
      },
    ])
  })

  it('returns [] when website is missing', async () => {
    expect(
      await projectWebsite({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await projectWebsite({
        name: 'proj',
        slug: 'proj',
        website: 'https://example.com',
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

describe('serpStrategy', () => {
  it('returns [] and makes no fetch call when there is no api key', async () => {
    const fetchMock = throwingFetch()
    const result = await serpStrategy({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })
    expect(result).toEqual([])
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('filters out non-docs and github.com results, keeps live docs results', async () => {
    routeFetch([
      [
        'https://serpapi.com/search.json',
        () =>
          Response.json({
            organic_results: [
              { link: 'https://docs.proj.dev/', title: 'proj documentation' },
              { link: 'https://github.com/example/proj', title: 'proj repo' },
              { link: 'https://proj.dev/blog', title: 'unrelated blog post' },
            ],
          }),
      ],
      ['https://docs.proj.dev', html],
    ])

    const result = await serpStrategy({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: 'key123',
    })
    expect(result).toEqual([
      { url: 'https://docs.proj.dev/', method: 'serp', confidence: 'low', livenessOk: true },
    ])
  })

  it('filters out www.github.com results', async () => {
    routeFetch([
      [
        'https://serpapi.com/search.json',
        () =>
          Response.json({
            organic_results: [{ link: 'https://www.github.com/example/proj', title: 'proj repo' }],
          }),
      ],
    ])

    const result = await serpStrategy({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: 'key123',
    })
    expect(result).toEqual([])
  })

  it('drops results whose host is unrelated to the project or is a noise host', async () => {
    routeFetch([
      [
        'https://serpapi.com/search.json',
        () =>
          Response.json({
            organic_results: [
              { link: 'https://redis.io/docs/latest/', title: 'proj documentation' },
              { link: 'https://en.wikipedia.org/wiki/proj-docs', title: 'proj documentation' },
              { link: 'https://www.linkedin.com/proj/docs', title: 'proj documentation' },
              { link: 'https://proj.readthedocs.io/en/latest/', title: 'proj documentation' },
            ],
          }),
      ],
      ['https://proj.readthedocs.io/en/latest/', html],
    ])

    const result = await serpStrategy({
      name: 'proj',
      slug: 'proj',
      website: null,
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: 'key123',
    })
    expect(result.map((c) => c.url)).toEqual(['https://proj.readthedocs.io/en/latest/'])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await serpStrategy({
        name: 'proj',
        slug: 'proj',
        website: null,
        websiteShared: false,
        repos: [],
        githubToken: null,
        serpApiKey: 'key123',
      }),
    ).toEqual([])
  })
})

describe('repoUrl', () => {
  const base = {
    name: 'Marquez',
    slug: 'marquez',
    website: null,
    websiteShared: false,
    githubToken: null,
    serpApiKey: null,
  }

  it('returns the canonical primary GitHub repo as a live low-confidence candidate', async () => {
    const result = await repoUrl({
      ...base,
      repos: [
        { url: 'https://github.com/MarquezProject/marquez.git', starCount: 10 },
        { url: 'https://github.com/MarquezProject/other', starCount: 500 },
      ],
    })

    expect(result).toEqual([
      {
        url: 'https://github.com/marquezproject/marquez',
        method: 'repo-url',
        confidence: 'low',
        livenessOk: true,
      },
    ])
  })

  it('returns nothing without a GitHub repo', async () => {
    expect(await repoUrl({ ...base, repos: [] })).toEqual([])
    expect(
      await repoUrl({ ...base, repos: [{ url: 'https://gitlab.com/org/repo', starCount: null }] }),
    ).toEqual([])
  })
})

describe('discoverDocs repo-url last resort', () => {
  const ctx = {
    name: 'Solo',
    slug: 'solo',
    website: null,
    websiteShared: false,
    repos: [{ url: 'https://github.com/acme/solo', starCount: 1 }],
    githubToken: null,
    serpApiKey: null,
  }

  it('resolves a repo-only project to its repo url with method repo-url', async () => {
    throwingFetch()

    const result = await discoverDocs(ctx)

    expect(result.docsUrl).toBe('https://github.com/acme/solo')
    expect(result.discoveryMethod).toBe('repo-url')
    expect(result.confidence).toBe('low')
  })

  it('does not stop SERP from running, since a repo page is not a docs signal', async () => {
    const fetchMock = routeFetch([
      [
        'https://serpapi.com/search.json',
        () =>
          Response.json({
            organic_results: [{ link: 'https://docs.solo.dev', title: 'Solo documentation' }],
          }),
      ],
      ['https://docs.solo.dev', html],
    ])

    const result = await discoverDocs({ ...ctx, serpApiKey: 'key' })

    expect(fetchMock).toHaveBeenCalled()
    expect(result.docsUrl).toBe('https://docs.solo.dev')
    expect(result.discoveryMethod).toBe('serp')
  })
})
