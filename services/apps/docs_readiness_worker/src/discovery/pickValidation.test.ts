// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import type { DocsVerdict, IDocsValidation } from './docsValidator'
import { discoverDocs } from './index'
import { extractPageEvidence } from './pickValidation'
import type { DocsPickValidator, IDiscoveryContext } from './strategies'

const mocks = vi.hoisted(() => ({
  STRATEGIES: [] as Array<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>,
  serpStrategy: vi.fn<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>(),
  probe: vi.fn(),
  fetchText: vi.fn(),
}))

vi.mock('./docsRoot', async () => {
  const actual = await vi.importActual<typeof import('./docsRoot')>('./docsRoot')
  return {
    ...actual,
    cutToDocsRoot: async (url: string) => url,
    cutSerpToDocsRoot: async (url: string) => url,
  }
})

vi.mock('./http', async () => {
  const actual = await vi.importActual<typeof import('./http')>('./http')
  return { ...actual, probe: mocks.probe, fetchText: mocks.fetchText }
})

vi.mock('./strategies', async () => {
  const actual = await vi.importActual<typeof import('./strategies')>('./strategies')
  return {
    ...actual,
    get STRATEGIES() {
      return mocks.STRATEGIES
    },
    serpStrategy: mocks.serpStrategy,
  }
})

const REPO_URL = 'https://github.com/example/proj'

function candidate(url: string, method: IDocCandidate['method']): IDocCandidate {
  return { url, method, confidence: 'medium', livenessOk: true }
}

function fakeValidator(...verdicts: DocsVerdict[]) {
  const queue = [...verdicts]
  return vi.fn<DocsPickValidator>(async (): Promise<IDocsValidation> => ({
    verdict: queue.shift() ?? 'unclear',
    reason: 'test',
  }))
}

function ctxWith(
  docsValidator: DocsPickValidator | null | undefined,
  log?: IDiscoveryContext['log'],
) {
  return {
    name: 'Example Project',
    slug: 'proj',
    website: 'https://example.com',
    websiteShared: false,
    repos: [{ url: REPO_URL, starCount: null }],
    githubToken: null,
    serpApiKey: null,
    docsValidator,
    log,
  } satisfies IDiscoveryContext
}

function withCandidates(...candidates: IDocCandidate[]) {
  mocks.STRATEGIES.push(async () => candidates)
}

beforeEach(() => {
  mocks.probe.mockImplementation(async (url: string) => ({
    ok: true,
    status: 200,
    finalUrl: url,
    contentType: 'text/html',
  }))
  mocks.fetchText.mockResolvedValue(
    '<html><head><title>Example docs</title></head><body><h1>Example</h1><p>Hello</p></body></html>',
  )
})

afterEach(() => {
  mocks.STRATEGIES.length = 0
  mocks.serpStrategy.mockReset()
  mocks.probe.mockReset()
  mocks.fetchText.mockReset()
})

describe('discoverDocs with the docs validator', () => {
  test.each([undefined, null])('flag off (%s): no fetch, no model call, same pick', async (off) => {
    withCandidates(
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate('https://example.com', 'project-website'),
    )

    const result = await discoverDocs(ctxWith(off))

    expect(result.docsUrl).toBe('https://example.com/docs')
    expect(result.discoveryMethod).toBe('readme-scrape')
    expect(mocks.fetchText).not.toHaveBeenCalled()
    expect(mocks.probe).not.toHaveBeenCalled()
  })

  test('accepts a README winner the validator confirms and sends page evidence', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))
    mocks.probe.mockResolvedValue({
      ok: true,
      status: 200,
      finalUrl: 'https://example.com/docs/',
      contentType: 'text/html',
    })
    const validator = fakeValidator('documents_project')

    const result = await discoverDocs(ctxWith(validator))

    expect(result.docsUrl).toBe('https://example.com/docs')
    expect(validator).toHaveBeenCalledTimes(1)
    expect(validator).toHaveBeenCalledWith(
      { name: 'Example Project', website: 'https://example.com', repoUrl: REPO_URL },
      expect.objectContaining({
        finalUrl: 'https://example.com/docs/',
        status: 200,
        title: 'Example docs',
        h1: 'Example',
      }),
    )
  })

  test('validates a search winner when no organic candidate is live', async () => {
    withCandidates(candidate(REPO_URL, 'repo-url'), candidate('https://elsewhere.dev/x', 'serp'))
    const validator = fakeValidator('other')

    const result = await discoverDocs(ctxWith(validator))

    expect(validator).toHaveBeenCalledTimes(1)
    expect(result.discoveryMethod).toBe('repo-url')
    expect(result.docsUrl).toBe(REPO_URL)
  })

  test.each<DocsVerdict>(['other', 'unclear'])(
    'a %s README winner is replaced by the next candidate, accepted unchecked',
    async (verdict) => {
      withCandidates(
        candidate('https://example.com/docs', 'readme-scrape'),
        candidate('https://example.com', 'project-website'),
      )
      const validator = fakeValidator(verdict)

      const result = await discoverDocs(ctxWith(validator))

      expect(result.docsUrl).toBe('https://example.com')
      expect(result.discoveryMethod).toBe('project-website')
      expect(validator).toHaveBeenCalledTimes(1)
    },
  )

  test('rejected README winner with nothing else live falls back to the repo URL', async () => {
    withCandidates(
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )

    const result = await discoverDocs(ctxWith(fakeValidator('unclear')))

    expect(result.docsUrl).toBe(REPO_URL)
    expect(result.discoveryMethod).toBe('repo-url')
  })

  test('rejected README winner with nothing else live resolves to no URL', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))

    const result = await discoverDocs(ctxWith(fakeValidator('other')))

    expect(result.docsUrl).toBeNull()
    expect(result.discoveryMethod).toBeNull()
    expect(result.confidence).toBeNull()
    expect(result.allCandidates).toHaveLength(1)
  })

  test.each<IDocCandidate['method']>([
    'docs-subdomain',
    'docs-path',
    'llms-txt-probe',
    'github-homepage',
    'project-website',
    'package-manifest',
    'repo-url',
  ])('a %s winner is never sent to the validator', async (method) => {
    withCandidates(candidate(method === 'repo-url' ? REPO_URL : 'https://example.com/docs', method))
    const validator = fakeValidator('other')

    const result = await discoverDocs(ctxWith(validator))

    expect(result.discoveryMethod).toBe(method)
    expect(validator).not.toHaveBeenCalled()
    expect(mocks.fetchText).not.toHaveBeenCalled()
  })

  test('makes at most 2 model calls and drops a third unchecked pick', async () => {
    withCandidates(
      candidate('https://docs.aaa.dev/docs', 'readme-scrape'),
      candidate('https://docs.bbb.dev/docs', 'readme-scrape'),
      candidate('https://docs.ccc.dev/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )
    const validator = fakeValidator('other', 'other', 'documents_project')

    const result = await discoverDocs(ctxWith(validator))

    expect(validator).toHaveBeenCalledTimes(2)
    expect(result.discoveryMethod).toBe('repo-url')
    expect(result.docsUrl).toBe(REPO_URL)
  })

  test('a second README pick is validated and accepted after the first is rejected', async () => {
    withCandidates(
      candidate('https://docs.aaa.dev/docs', 'readme-scrape'),
      candidate('https://docs.bbb.dev/docs', 'readme-scrape'),
    )
    const validator = fakeValidator('other', 'documents_project')

    const result = await discoverDocs(ctxWith(validator))

    expect(validator).toHaveBeenCalledTimes(2)
    expect(result.discoveryMethod).toBe('readme-scrape')
    expect(result.docsUrl).not.toBeNull()
  })

  test('fails open when the validator throws, and logs without page text', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))
    const log = { info: vi.fn(), warn: vi.fn() }
    const validator = vi.fn<DocsPickValidator>().mockRejectedValue(new Error('secret-key-123'))

    const result = await discoverDocs(ctxWith(validator, log))

    expect(result.docsUrl).toBe('https://example.com/docs')
    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(JSON.stringify(log.warn.mock.calls)).not.toContain('secret-key-123')
  })

  test('fails open when the page fetch throws', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))
    mocks.fetchText.mockRejectedValue(new Error('boom'))
    const validator = fakeValidator('other')

    const result = await discoverDocs(ctxWith(validator))

    expect(result.docsUrl).toBe('https://example.com/docs')
    expect(validator).not.toHaveBeenCalled()
  })

  test('keeps the pick without a model call when no page evidence can be fetched', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))
    mocks.fetchText.mockResolvedValue(null)
    const validator = fakeValidator('other')

    const result = await discoverDocs(ctxWith(validator))

    expect(result.docsUrl).toBe('https://example.com/docs')
    expect(validator).not.toHaveBeenCalled()
  })

  test('logs one structured line per validator call', async () => {
    withCandidates(
      candidate('https://docs.aaa.dev/docs', 'readme-scrape'),
      candidate('https://docs.bbb.dev/docs', 'readme-scrape'),
    )
    const log = { info: vi.fn(), warn: vi.fn() }

    await discoverDocs(ctxWith(fakeValidator('other', 'documents_project'), log))

    expect(log.info).toHaveBeenCalledTimes(2)
    expect(log.info.mock.calls.map(([fields]) => Object.keys(fields).sort())).toEqual([
      ['method', 'slug', 'url', 'verdict'],
      ['method', 'slug', 'url', 'verdict'],
    ])
    expect(log.info.mock.calls.map(([fields]) => fields.verdict)).toEqual([
      'other',
      'documents_project',
    ])
  })
})

describe('known wrong README picks are rejected by the validator', () => {
  test.each([
    'https://community.finos.org/docs/governance/Software-Projects/easycla',
    'https://www.graphql-js.org/',
    'https://docs.spring.io/spring-framework/docs/6.0.x/reference/html/web.html',
    'https://docs.developers.symphony.com/building-bots-on-symphony/datafeed/real-time-events',
    'https://uxlfoundation.github.io/oneTBB',
  ])('%s is dropped', async (url) => {
    withCandidates(candidate(url, 'readme-scrape'), candidate(REPO_URL, 'repo-url'))
    const validator = fakeValidator('other')

    const result = await discoverDocs(ctxWith(validator))

    expect(validator).toHaveBeenCalledTimes(1)
    expect(result.docsUrl).toBe(REPO_URL)
    expect(result.discoveryMethod).toBe('repo-url')
  })
})

describe('extractPageEvidence', () => {
  test('reads title, first h1 and meta description, and strips scripts and tags from the text', () => {
    const html = `<!doctype html><html><head><title> My &amp; Docs </title>
      <meta content="Fast things" name="description"><style>p{color:red}</style>
      <script>var secret = "x"</script></head>
      <body><!-- hidden --><h1>Welcome <em>home</em></h1><h1>Second</h1><p>Some&nbsp;text</p></body></html>`

    const page = extractPageEvidence(html, 'https://x.dev/docs', 200)

    expect(page).toMatchObject({
      finalUrl: 'https://x.dev/docs',
      status: 200,
      title: 'My & Docs',
      h1: 'Welcome home',
      description: 'Fast things',
    })
    expect(page.text).toContain('Welcome home')
    expect(page.text).toContain('Some text')
    expect(page.text).not.toMatch(/secret|color:red|hidden|<|>/)
  })

  test('caps the visible text at 1500 characters and tolerates missing parts', () => {
    const page = extractPageEvidence(`<body>${'word '.repeat(2000)}</body>`, 'https://x.dev', 404)

    expect(page.text).toHaveLength(1500)
    expect(page.title).toBeNull()
    expect(page.h1).toBeNull()
    expect(page.description).toBeNull()
    expect(page.status).toBe(404)
  })
})
