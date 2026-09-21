import { afterEach, describe, expect, test, vi } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import { discoverDocs } from './index'
import type { IDiscoveryContext } from './strategies'

const strategyMocks = vi.hoisted(() => ({
  STRATEGIES: [] as Array<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>,
  serpStrategy: vi.fn<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>(),
}))

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
  return { name: 'proj', website: 'https://example.com', repos: [], githubToken: null, serpApiKey }
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

  test('falls back to serp when the only live candidate has no docs signal', async () => {
    strategyMocks.STRATEGIES.push(async () => [
      candidate('https://example.com', 'project-website', true),
    ])
    strategyMocks.serpStrategy.mockResolvedValue([
      candidate('https://docs.example.com/guide', 'serp', true),
    ])

    const result = await discoverDocs(ctx('serp-key'))

    expect(strategyMocks.serpStrategy).toHaveBeenCalledTimes(1)
    expect(result.docsUrl).toBe('https://docs.example.com/guide')
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
      website: 'https://github.com/acme/real-project',
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
      website: 'https://github.com/acme/real-project',
      repos: [],
      githubToken: null,
      serpApiKey: null,
    })

    expect(result.docsUrl).toBe('https://acme-widgets.io/docs')
  })
})
