import { afterEach, describe, expect, it, vi } from 'vitest'

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
      website: 'https://example.com',
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
      website: 'https://example.com',
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
      website: 'https://example.com',
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
        website: null,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await llmsTxtProbe({
        name: 'proj',
        website: 'https://example.com',
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
      website: 'https://example.com',
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
        website: 'https://example.com',
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
        website: null,
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await docsSubdomain({
        name: 'proj',
        website: 'https://example.com',
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
      website: 'https://example.com',
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

  it('returns [] when website is missing', async () => {
    expect(
      await docsPath({
        name: 'proj',
        website: null,
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
        website: 'https://example.com',
        repos: [],
        githubToken: null,
        serpApiKey: null,
      }),
    ).toEqual([])
  })
})

const repos = ['https://github.com/torvalds/linux']

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
      website: null,
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
        website: null,
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
        website: null,
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
        website: null,
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
      website: null,
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
        website: null,
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
        website: null,
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
        website: null,
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
      ['https://docs.example.com', html],
    ])

    const result = await githubHomepage({
      name: 'proj',
      website: null,
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
        website: null,
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
        website: null,
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
        website: null,
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
        website: null,
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
      website: 'https://example.com',
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
        website: null,
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
        website: 'https://example.com',
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
      website: null,
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
      website: null,
      repos: [],
      githubToken: null,
      serpApiKey: 'key123',
    })
    expect(result).toEqual([
      { url: 'https://docs.example.com/', method: 'serp', confidence: 'low', livenessOk: true },
    ])
  })

  it('returns [] on a fetch error', async () => {
    throwingFetch()
    expect(
      await serpStrategy({
        name: 'proj',
        website: null,
        repos: [],
        githubToken: null,
        serpApiKey: 'key123',
      }),
    ).toEqual([])
  })
})
