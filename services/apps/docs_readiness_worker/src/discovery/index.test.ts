import { afterEach, describe, expect, test, vi } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import { discoverDocs } from './index'
import type { IDiscoveryContext } from './strategies'

const strategyMocks = vi.hoisted(() => ({
  STRATEGIES: [] as Array<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>,
  serpStrategy: vi.fn<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>(),
}))

vi.mock('./docsRoot', async () => {
  const actual = await vi.importActual<typeof import('./docsRoot')>('./docsRoot')
  return {
    ...actual,
    cutToDocsRoot: async (url: string) => actual.cutAtVersion(url) ?? url,
    cutSerpToDocsRoot: async (url: string) => actual.cutAtDocsSegment(url) ?? url,
  }
})

vi.mock('./strategies', async () => {
  const actual = await vi.importActual<typeof import('./strategies')>('./strategies')
  return {
    ...actual,
    get STRATEGIES() {
      return strategyMocks.STRATEGIES
    },
    serpStrategy: strategyMocks.serpStrategy,
  }
})

function ctx(serpApiKey: string | null = null): IDiscoveryContext {
  return {
    name: 'proj',
    slug: 'proj',
    website: 'https://example.com',
    websiteShared: false,
    repos: [],
    githubToken: null,
    serpApiKey,
  }
}

function candidate(
  url: string,
  method: IDocCandidate['method'],
  livenessOk: boolean,
): IDocCandidate {
  return { url, method, confidence: 'medium', livenessOk }
}

afterEach(() => {
  strategyMocks.STRATEGIES.length = 0
  strategyMocks.serpStrategy.mockReset()
})

describe('discoverDocs', () => {
  test('picks the ranked winner from the base 7 strategies', async () => {
    strategyMocks.STRATEGIES.push(
      async () => [candidate('https://example.com', 'project-website', true)],
      async () => [candidate('https://docs.example.com', 'docs-subdomain', true)],
    )

    const result = await discoverDocs(ctx())

    expect(result.docsUrl).toBe('https://docs.example.com')
    expect(result.discoveryMethod).toBe('docs-subdomain')
    expect(result.confidence).toBe('medium')
    expect(result.allCandidates).toHaveLength(2)
  })

  test('returns nulls and empty candidates when nothing is found', async () => {
    strategyMocks.STRATEGIES.push(async () => [])

    const result = await discoverDocs(ctx())

    expect(result).toEqual({
      docsUrl: null,
      discoveryMethod: null,
      confidence: null,
      allCandidates: [],
    })
  })

  test('tolerates a rejected strategy instead of failing the whole discovery', async () => {
    strategyMocks.STRATEGIES.push(
      async () => {
        throw new Error('boom')
      },
      async () => [candidate('https://docs.example.com', 'docs-subdomain', true)],
    )

    const result = await discoverDocs(ctx())

    expect(result.docsUrl).toBe('https://docs.example.com')
  })

  test('dedupes candidates that share the same URL across strategies', async () => {
    strategyMocks.STRATEGIES.push(
      async () => [candidate('https://docs.example.com', 'docs-subdomain', true)],
      async () => [candidate('https://docs.example.com', 'docs-path', true)],
    )

    const result = await discoverDocs(ctx())

    expect(result.allCandidates).toHaveLength(1)
    expect(result.allCandidates[0].method).toBe('docs-subdomain')
  })

  test('does not call serp when a live candidate already exists, even with a key set', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://docs.example.com', 'docs-subdomain', true),
    ])

    await discoverDocs(ctx('serp-key'))

    expect(strategyMocks.serpStrategy).not.toHaveBeenCalled()
  })

  test('does not call serp when no serpApiKey is set, even with no live candidates', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://example.com', 'project-website', false),
    ])

    await discoverDocs(ctx(null))

    expect(strategyMocks.serpStrategy).not.toHaveBeenCalled()
  })

  test('prefers a live duplicate over a dead one for the same URL across strategies', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://docs.example.com', 'docs-path', false),
    ])
    strategyMocks.serpStrategy.mockResolvedValue([
      candidate('https://docs.example.com', 'serp', true),
    ])

    const result = await discoverDocs(ctx('serp-key'))

    expect(result.allCandidates).toHaveLength(1)
    expect(result.allCandidates[0].livenessOk).toBe(true)
    expect(result.docsUrl).toBe('https://docs.example.com')
  })

  test('does not call serp when the only live candidate is a signal-less homepage (5 Spot)', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://5spot.finos.org/', 'github-homepage', true),
      candidate('https://github.com/finos/5-spot', 'repo-url', true),
    ])
    strategyMocks.serpStrategy.mockResolvedValue([
      candidate(
        'https://docs.buildbot.net/2.0.1/manual/configuration/schedulers.html',
        'serp',
        true,
      ),
    ])

    const result = await discoverDocs({ ...ctx('serp-key'), website: null })

    expect(strategyMocks.serpStrategy).not.toHaveBeenCalled()
    expect(result.docsUrl).toBe('https://5spot.finos.org/')
    expect(result.discoveryMethod).toBe('github-homepage')
  })

  test('calls serp when the only live candidate is the repo-url fallback', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://github.com/acme/proj', 'repo-url', true),
    ])
    strategyMocks.serpStrategy.mockResolvedValue([
      candidate('https://proj.readthedocs.io/en/latest', 'serp', true),
    ])

    const result = await discoverDocs({ ...ctx('serp-key'), website: null })

    expect(strategyMocks.serpStrategy).toHaveBeenCalledTimes(1)
    expect(result.docsUrl).toBe('https://proj.readthedocs.io/')
  })

  test('cuts a deep serp winner to its docs root', async () => {
    strategyMocks.STRATEGIES.push(async () => [])
    strategyMocks.serpStrategy.mockResolvedValue([
      candidate(
        'https://rasa.com/docs/studio/build/content-management/buttons-and-links/',
        'serp',
        true,
      ),
    ])

    const result = await discoverDocs({ ...ctx('serp-key'), website: null })

    expect(result.docsUrl).toBe('https://rasa.com/docs')
    expect(result.discoveryMethod).toBe('serp')
  })

  test('leaves a non-serp winner at its own path', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://example.com/guides/install/linux', 'readme-scrape', true),
    ])

    const result = await discoverDocs(ctx())

    expect(result.docsUrl).toBe('https://example.com/guides/install/linux')
  })

  test('ranks with the project website domain, favoring it over an unrelated better-shaped domain', async () => {
    strategyMocks.STRATEGIES.push(
      async () => [
        candidate('https://example.com', 'github-homepage', true),
        candidate('https://example.com/docs', 'readme-scrape', true),
      ],
      async () => [
        candidate('https://docs.unrelated-vendor.com/reference', 'package-manifest', true),
      ],
    )

    const result = await discoverDocs(ctx())

    expect(result.docsUrl).toBe('https://example.com/docs')
  })

  test('falls back to serp only when no live candidate exists and a key is set', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://example.com', 'project-website', false),
    ])
    strategyMocks.serpStrategy.mockResolvedValue([
      candidate('https://docs.other.com', 'serp', true),
    ])

    const result = await discoverDocs(ctx('serp-key'))

    expect(strategyMocks.serpStrategy).toHaveBeenCalledTimes(1)
    expect(result.docsUrl).toBe('https://docs.other.com')
    expect(result.allCandidates).toHaveLength(2)
  })

  test('does not treat a github.com repo url website as a project domain for affinity', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://docs.github.com', 'docs-subdomain', true),
      candidate('https://docs.realproject.dev', 'llms-txt-probe', true),
    ])

    const result = await discoverDocs({
      name: 'proj',
      slug: 'proj',
      website: 'https://github.com/acme/real-project',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })

    expect(result.docsUrl).toBe('https://docs.realproject.dev')
  })

  test('narrows to the repo owner name when a live candidate is actually on that domain', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://acme-widgets.io/docs', 'llms-txt-probe', true),
      candidate('https://docs.unrelated-vendor.com/reference', 'llms-txt-probe', true),
    ])

    const result = await discoverDocs({
      name: 'proj',
      slug: 'proj',
      website: 'https://github.com/acme/real-project',
      websiteShared: false,
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })

    expect(result.docsUrl).toBe('https://acme-widgets.io/docs')
  })
})

describe('discoverDocs ranking inputs (IN-1393)', () => {
  const foundationCtx = (over: Partial<IDiscoveryContext> = {}): IDiscoveryContext => ({
    ...ctx(),
    website: 'https://foundation.org/projects/x',
    websiteShared: true,
    ...over,
  })

  test('a shared website is not used as the project domain for affinity', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://foundation.org/docs', 'docs-path', true),
      candidate('https://docs.x-project.dev', 'docs-subdomain', true),
    ])

    const shared = await discoverDocs(foundationCtx())
    expect(shared.docsUrl).toBe('https://docs.x-project.dev')

    // Same candidates with an unshared website anchor the pool on its own domain instead.
    const own = await discoverDocs(foundationCtx({ websiteShared: false }))
    expect(own.docsUrl).toBe('https://foundation.org/docs')
  })

  test('a candidate found by findSharedDocsUrls loses to an unshared one', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://docs.lfenergy.org', 'docs-subdomain', true),
      candidate('https://myproject.org', 'project-website', true),
    ])

    const result = await discoverDocs({
      ...ctx(),
      website: null,
      findSharedDocsUrls: async () => ['https://docs.lfenergy.org/'],
    })

    expect(result.docsUrl).toBe('https://myproject.org')
  })

  test('a deep versioned url is not cut to a root another project already claims', async () => {
    const deep = 'https://docs.example.org/v2/page'
    strategyMocks.STRATEGIES.push(async () => [candidate(deep, 'docs-subdomain', true)])
    const result = await discoverDocs({
      ...ctx(),
      website: null,
      findSharedDocsUrls: async () => ['https://docs.example.org/'],
    })

    expect(result.docsUrl).toBe(deep)
  })

  test('looks up shared URLs once, for the distinct hosts of the live candidates only', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://www.lfenergy.org/a', 'docs-path', true),
      candidate('https://lfenergy.org/b', 'docs-path', true),
      candidate('https://myproject.org', 'project-website', true),
      candidate('https://dead.example.com', 'docs-path', false),
    ])
    const findSharedDocsUrls = vi.fn(async () => [])

    await discoverDocs({ ...ctx(), website: null, findSharedDocsUrls })

    expect(findSharedDocsUrls).toHaveBeenCalledTimes(1)
    expect(findSharedDocsUrls).toHaveBeenCalledWith(['lfenergy.org', 'myproject.org'])
  })

  test('skips the shared URL lookup when no candidate is live', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://dead.example.com', 'docs-path', false),
    ])
    const findSharedDocsUrls = vi.fn(async () => [])

    const result = await discoverDocs({ ...ctx(), website: null, findSharedDocsUrls })

    expect(result.docsUrl).toBeNull()
    expect(findSharedDocsUrls).not.toHaveBeenCalled()
  })

  test('a foundation llms.txt root loses to the repo homepage page (report case 10)', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://openmainframeproject.org', 'llms-txt-probe', true),
      candidate(
        'https://openmainframeproject.org/projects/cobol-programming-course',
        'github-homepage',
        true,
      ),
    ])

    const result = await discoverDocs(
      foundationCtx({ website: 'https://openmainframeproject.org/projects/cobol' }),
    )

    expect(result.docsUrl).toBe(
      'https://openmainframeproject.org/projects/cobol-programming-course',
    )
  })
})
