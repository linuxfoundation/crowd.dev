// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import type { DocsVerdict, IDocsValidation } from './docsValidator'
import { discoverDocs } from './index'
import { MIN_VALIDATION_BUDGET_MS, extractPageEvidence } from './pickValidation'
import type { DocsPickValidator, IDiscoveryContext } from './strategies'

const mocks = vi.hoisted(() => ({
  STRATEGIES: [] as Array<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>,
  serpStrategy: vi.fn<(ctx: IDiscoveryContext) => Promise<IDocCandidate[]>>(),
  probe: vi.fn(),
  fetchText: vi.fn(),
  cutDelayMs: 0,
}))

vi.mock('./docsRoot', async () => {
  const actual = await vi.importActual<typeof import('./docsRoot')>('./docsRoot')
  return {
    ...actual,
    cutToDocsRoot: async (url: string) => {
      await new Promise((resolve) => setTimeout(resolve, mocks.cutDelayMs))
      return url
    },
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
  mocks.cutDelayMs = 0
  mocks.STRATEGIES.length = 0
  mocks.serpStrategy.mockReset()
  mocks.probe.mockReset()
  mocks.fetchText.mockReset()
})

describe('discoverDocs with the docs validator', () => {
  test('flag off: the complete result equals the unflagged one, with no fetch at all', async () => {
    const candidates = [
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate('https://example.com', 'project-website'),
    ]
    withCandidates(...candidates)

    const unflagged = await discoverDocs(ctxWith(undefined))
    const nulled = await discoverDocs(ctxWith(null))

    expect(unflagged).toEqual({
      docsUrl: 'https://example.com/docs',
      discoveryMethod: 'readme-scrape',
      confidence: 'medium',
      allCandidates: candidates,
    })
    expect(nulled).toEqual(unflagged)
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

  test('an other README winner is replaced by the next candidate, accepted unchecked', async () => {
    withCandidates(
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate('https://example.com', 'project-website'),
    )
    const validator = fakeValidator('other')

    const result = await discoverDocs(ctxWith(validator))

    expect(result.docsUrl).toBe('https://example.com')
    expect(result.discoveryMethod).toBe('project-website')
    expect(validator).toHaveBeenCalledTimes(1)
  })

  test.each<[string, string, Partial<IDiscoveryContext>]>([
    [
      'the project website',
      'https://docs.example.com/guide',
      {
        name: 'Zed',
        slug: 'zed',
        repos: [{ url: 'https://github.com/acme/zed', starCount: null }],
      },
    ],
    [
      'the repo owner',
      'http://help.openfido.org/',
      {
        name: 'FIDOPower',
        slug: 'fidopower',
        website: null,
        repos: [{ url: 'https://github.com/openfido/concatenate', starCount: null }],
      },
    ],
    ['the project name', 'https://example.dev/docs', { website: null, repos: [] }],
  ])('an unclear README winner on a domain matching %s is kept', async (_label, url, overrides) => {
    withCandidates(candidate(url, 'readme-scrape'), candidate(REPO_URL, 'repo-url'))
    const log = { info: vi.fn(), warn: vi.fn() }

    const result = await discoverDocs({ ...ctxWith(fakeValidator('unclear'), log), ...overrides })

    expect(result.docsUrl).toBe(url)
    expect(result.discoveryMethod).toBe('readme-scrape')
    expect(log.info.mock.calls.map(([fields]) => fields.outcome)).toContain('unclear-related')
  })

  test('an unclear README winner on an unrelated domain is dropped', async () => {
    withCandidates(
      candidate('https://docs.other-product.dev/guide', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )

    const result = await discoverDocs(ctxWith(fakeValidator('unclear')))

    expect(result.docsUrl).toBe(REPO_URL)
    expect(result.discoveryMethod).toBe('repo-url')
  })

  test('an unclear winner on a shared website is not kept for sharing its domain', async () => {
    withCandidates(
      candidate('https://docs.umbrella.org/other-project', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )

    const result = await discoverDocs({
      ...ctxWith(fakeValidator('unclear')),
      website: 'https://www.umbrella.org',
      websiteShared: true,
    })

    expect(result.discoveryMethod).toBe('repo-url')
  })

  test.each([
    ['a multi-tenant host', 'https://docs.rs/foo', 'https://docs.rs/bar'],
    [
      'an umbrella host',
      'https://wiki.linuxfoundation.org/foo',
      'https://events.linuxfoundation.org/bar',
    ],
  ])(
    'an unclear winner on the same domain as a website on %s is dropped',
    async (_l, website, url) => {
      withCandidates(candidate(url, 'readme-scrape'), candidate(REPO_URL, 'repo-url'))

      const result = await discoverDocs({ ...ctxWith(fakeValidator('unclear')), website })

      expect(result.discoveryMethod).toBe('repo-url')
    },
  )

  test('an other verdict drops a winner even on a matching domain', async () => {
    withCandidates(
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )

    const result = await discoverDocs(ctxWith(fakeValidator('other')))

    expect(result.discoveryMethod).toBe('repo-url')
  })

  test('rejected README winner with nothing else live falls back to the repo URL', async () => {
    withCandidates(
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )

    const result = await discoverDocs(ctxWith(fakeValidator('other')))

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

  test('makes at most 3 model calls and drops a fourth unchecked pick', async () => {
    withCandidates(
      candidate('https://docs.aaa.dev/docs', 'readme-scrape'),
      candidate('https://docs.bbb.dev/docs', 'readme-scrape'),
      candidate('https://docs.ccc.dev/docs', 'readme-scrape'),
      candidate('https://docs.ddd.dev/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )
    const validator = fakeValidator('other', 'other', 'other', 'documents_project')

    const result = await discoverDocs(ctxWith(validator))

    expect(validator).toHaveBeenCalledTimes(3)
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

  test('logs a dropped pick once when the call cap is reached, with an outcome and no verdict', async () => {
    withCandidates(
      candidate('https://docs.aaa.dev/docs', 'readme-scrape'),
      candidate('https://docs.bbb.dev/docs', 'readme-scrape'),
      candidate('https://docs.ccc.dev/docs', 'readme-scrape'),
      candidate('https://docs.ddd.dev/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )
    const log = { info: vi.fn(), warn: vi.fn() }

    await discoverDocs(ctxWith(fakeValidator('other', 'other', 'other'), log))

    const capped = log.info.mock.calls.filter(([fields]) => fields.outcome === 'call-cap')
    expect(capped).toHaveLength(1)
    expect(capped[0][0]).not.toHaveProperty('verdict')
  })

  test('no page evidence is logged as an outcome, never as a verdict', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))
    mocks.fetchText.mockResolvedValue(null)
    const log = { info: vi.fn(), warn: vi.fn() }

    await discoverDocs(ctxWith(fakeValidator('other'), log))

    expect(log.info).toHaveBeenCalledTimes(1)
    expect(log.info.mock.calls[0][0]).toMatchObject({ outcome: 'no-page-evidence' })
    expect(log.info.mock.calls[0][0]).not.toHaveProperty('verdict')
  })

  test('a validator error is logged as an outcome with only the error name', async () => {
    withCandidates(candidate('https://example.com/docs', 'readme-scrape'))
    const log = { info: vi.fn(), warn: vi.fn() }
    const boom = Object.assign(new Error('secret-key-123'), { name: 'BoomError' })

    await discoverDocs(ctxWith(vi.fn<DocsPickValidator>().mockRejectedValue(boom), log))

    expect(log.warn.mock.calls[0][0]).toEqual({
      slug: 'proj',
      method: 'readme-scrape',
      url: 'https://example.com/docs',
      outcome: 'validator-error',
      errorName: 'BoomError',
    })
  })
})

describe('known wrong README picks are rejected by the validator', () => {
  const WRONG = [
    'https://community.finos.org/docs/governance/Software-Projects/easycla',
    'https://www.graphql-js.org/',
    'https://docs.spring.io/spring-framework/docs/6.0.x/reference/html/web.html',
    'https://docs.developers.symphony.com/building-bots-on-symphony/datafeed/real-time-events',
    'https://uxlfoundation.github.io/oneTBB',
  ]
  // Decides from the page it is shown, like the model does, not from the call order.
  const byFinalUrl = () =>
    vi.fn<DocsPickValidator>(async (_project, page): Promise<IDocsValidation> => ({
      verdict: WRONG.includes(page.finalUrl) ? 'other' : 'documents_project',
      reason: 'test',
    }))

  test.each(WRONG)('%s is dropped', async (url) => {
    withCandidates(candidate(url, 'readme-scrape'), candidate(REPO_URL, 'repo-url'))
    const validator = byFinalUrl()

    const result = await discoverDocs(ctxWith(validator))

    expect(validator).toHaveBeenCalledTimes(1)
    expect(validator.mock.calls[0][1].finalUrl).toBe(url)
    expect(result.docsUrl).toBe(REPO_URL)
    expect(result.discoveryMethod).toBe('repo-url')
  })

  test('a correct README pick is kept', async () => {
    withCandidates(
      candidate('https://example.com/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    )

    const result = await discoverDocs(ctxWith(byFinalUrl()))

    expect(result.docsUrl).toBe('https://example.com/docs')
    expect(result.discoveryMethod).toBe('readme-scrape')
  })
})

describe('validation time bound', () => {
  // DISCOVERY_TIMEOUT_MS in activities/discovery.ts is 240 s.
  const DISCOVERY_MS = 240_000
  // Hosts answer only at their 5 s evidence timeout, the model only at its 20 s timeout and the
  // docs-root cut probe only at its 10 s timeout: the slowest case every step can have.
  const EVIDENCE_MS = 5_000
  const MODEL_MS = 20_000
  const CUT_MS = 10_000

  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

  afterEach(() => {
    vi.useRealTimers()
  })

  async function run(discoveryTakesMs: number) {
    vi.useFakeTimers()
    mocks.cutDelayMs = CUT_MS
    mocks.probe.mockImplementation(async (url: string) => {
      await sleep(EVIDENCE_MS)
      return { ok: true, status: 200, finalUrl: url, contentType: 'text/html' }
    })
    mocks.fetchText.mockImplementation(async () => {
      await sleep(EVIDENCE_MS)
      return '<title>Other</title>'
    })
    const candidates = [
      candidate('https://docs.aaa.dev/docs', 'readme-scrape'),
      candidate('https://docs.bbb.dev/docs', 'readme-scrape'),
      candidate('https://docs.ccc.dev/docs', 'readme-scrape'),
      candidate(REPO_URL, 'repo-url'),
    ]
    mocks.STRATEGIES.push(async () => {
      await sleep(discoveryTakesMs)
      return candidates
    })
    const validator = vi.fn<DocsPickValidator>(async () => {
      await sleep(MODEL_MS)
      return { verdict: 'other', reason: 'test' }
    })
    const log = { info: vi.fn(), warn: vi.fn() }

    const startedAt = Date.now()
    let elapsedMs = -1
    const done = discoverDocs({
      ...ctxWith(validator, log),
      deadlineAt: startedAt + DISCOVERY_MS,
    }).then((result) => {
      elapsedMs = Date.now() - startedAt
      return result
    })
    await vi.advanceTimersByTimeAsync(600_000)
    return { result: await done, elapsedMs, validator, log }
  }

  test('three validated picks after 150 s of discovery finish by 235 s', async () => {
    const { result, elapsedMs, validator } = await run(150_000)

    expect(validator).toHaveBeenCalledTimes(3)
    expect(result.discoveryMethod).toBe('repo-url')
    expect(elapsedMs).toBe(235_000)
  })

  test('the last validation that still fits leaves the cut probe under the bound', async () => {
    const { elapsedMs, validator, log } = await run(DISCOVERY_MS - MIN_VALIDATION_BUDGET_MS - 1_000)

    expect(validator).toHaveBeenCalledTimes(1)
    expect(log.info.mock.calls.filter(([f]) => f.outcome === 'budget')).toHaveLength(1)
    // 199 + 5 + 20 + 10 cut: the second pick is kept unvalidated because 16 s remain
    expect(elapsedMs).toBe(DISCOVERY_MS - 6_000)
  })

  test('validation is skipped, once, when less than 40 s remain', async () => {
    const { result, elapsedMs, validator, log } = await run(
      DISCOVERY_MS - MIN_VALIDATION_BUDGET_MS + 1,
    )

    // the slow strategy finishes at 200.001 s, leaving 39.999 s
    expect(validator).not.toHaveBeenCalled()
    expect(mocks.fetchText).not.toHaveBeenCalled()
    expect(result.discoveryMethod).toBe('readme-scrape')
    expect(log.info.mock.calls.filter(([f]) => f.outcome === 'budget')).toHaveLength(1)
    expect(elapsedMs).toBe(DISCOVERY_MS - MIN_VALIDATION_BUDGET_MS + 1 + CUT_MS)
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

  test('finds tags case-insensitively and ignores look-alike names', () => {
    const html =
      '<TITLE>Big</TITLE><titlex>no</titlex><H1 class="a">Head</H1><META PROPERTY="og:description" CONTENT="Og text">'

    expect(extractPageEvidence(html, 'https://x.dev', 200)).toMatchObject({
      title: 'Big',
      h1: 'Head',
      description: 'Og text',
    })
  })

  test('drops an unterminated comment or script and keeps a stray less-than as text', () => {
    expect(extractPageEvidence('<p>kept</p><!-- never closed <p>gone</p>', 'u', 200).text).toBe(
      'kept',
    )
    expect(extractPageEvidence('<p>kept</p><script>var x = 1', 'u', 200).text).toBe('kept')
    expect(extractPageEvidence('<p>1 < 2 and more', 'u', 200).text).toBe('1 < 2 and more')
  })

  test('only reads the first 200000 characters', () => {
    const html = `${' '.repeat(200_000)}<title>Late</title>`

    expect(extractPageEvidence(html, 'u', 200).title).toBeNull()
  })

  // The scan is synchronous, so a slow one would stall the whole worker (see the IN-1398 freeze).
  test.each([
    ['only less-than signs', '<'.repeat(300_000)],
    ['less-than signs inside words', 'a<b '.repeat(75_000)],
    ['unclosed tags', '<a'.repeat(150_000)],
    ['unclosed title, meta and script', '<title><meta <h1 <script '.repeat(12_000)],
    ['many closed tags', '<i>x</i>'.repeat(40_000)],
  ])('extracts from adversarial html (%s) in well under a second', (_name, html) => {
    const startedAt = performance.now()

    const page = extractPageEvidence(html, 'https://x.dev', 200)

    expect(performance.now() - startedAt).toBeLessThan(1000)
    expect(page.text?.length).toBeLessThanOrEqual(1500)
  })
})
