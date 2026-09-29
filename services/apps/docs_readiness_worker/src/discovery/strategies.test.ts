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
  serpStrategy,
} from './strategies'

function routeFetch(routes: [string, () => Response][]) {
  const fetchMock = vi.fn((input: string | URL | Request) => {
    const url = typeof input === 'string' ? input : input.toString()
    const match = routes.find(([prefix]) => url.startsWith(prefix))
    if (!match) {
      return Promise.reject(new Error(`no route for ${url}`))
    }
    return Promise.resolve(match[1]())
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
              { link: 'https://docs.example.com/', title: 'proj documentation' },
              { link: 'https://github.com/example/proj', title: 'proj repo' },
              { link: 'https://example.com/blog', title: 'unrelated blog post' },
            ],
          }),
      ],
      ['https://docs.example.com', html],
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
      { url: 'https://docs.example.com/', method: 'serp', confidence: 'low', livenessOk: true },
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
