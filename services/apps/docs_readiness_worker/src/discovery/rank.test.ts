import { readFileSync } from 'fs'
import { join } from 'path'
import { describe, expect, test } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import { rankCandidates } from './rank'

const pocOutcomes = JSON.parse(
  readFileSync(join(__dirname, '__fixtures__/poc-rank-outcomes.json'), 'utf-8'),
)

function candidate(
  url: string,
  method: IDocCandidate['method'],
  livenessOk: boolean,
  confidence: IDocCandidate['confidence'] = 'medium',
): IDocCandidate {
  return { url, method, confidence, livenessOk }
}

describe('rankCandidates', () => {
  test('returns null when there are no candidates', () => {
    expect(rankCandidates([])).toBeNull()
  })

  test('returns null when no candidate is live', () => {
    const candidates = [candidate('https://docs.example.com', 'docs-subdomain', false)]
    expect(rankCandidates(candidates)).toBeNull()
  })

  test('ignores non-live candidates entirely', () => {
    const live = candidate('https://example.com', 'project-website', true)
    const dead = candidate('https://docs.example.com', 'docs-subdomain', false)
    expect(rankCandidates([dead, live])).toEqual(live)
  })

  test('prefers llms-txt-probe over every other method bonus, URL shape held equal', () => {
    const llms = candidate('https://docs.a.com', 'llms-txt-probe', true)
    const subdomain = candidate('https://docs.b.com', 'docs-subdomain', true)
    expect(rankCandidates([subdomain, llms])).toEqual(llms)
  })

  test('method bonus ordering: docs-subdomain > docs-path > serp/package-manifest > readme-scrape/github-homepage/project-website', () => {
    const subdomain = candidate('https://a.com', 'docs-subdomain', true)
    const path = candidate('https://b.com', 'docs-path', true)
    const serp = candidate('https://c.com', 'serp', true)
    const readme = candidate('https://d.com', 'readme-scrape', true)
    expect(rankCandidates([path, subdomain])).toEqual(subdomain)
    expect(rankCandidates([serp, path])).toEqual(path)
    expect(rankCandidates([readme, serp])).toEqual(serp)
  })

  test('URL shape bonus: a docs. host outranks a same-method bare host', () => {
    const bare = candidate('https://example.com', 'project-website', true)
    const docsHost = candidate('https://docs.example.com', 'project-website', true)
    expect(rankCandidates([bare, docsHost])).toEqual(docsHost)
  })

  test('URL shape bonus: a /docs path outranks a same-method bare path', () => {
    const bare = candidate('https://example.com/', 'github-homepage', true)
    const docsPath = candidate('https://example.com/docs', 'github-homepage', true)
    expect(rankCandidates([bare, docsPath])).toEqual(docsPath)
  })

  test('domain agreement bonus: two strategies pointing at the same domain beat a lone higher-bonus candidate on a different domain', () => {
    // docs-path (+3) alone on domain A vs. two low-bonus candidates (readme-scrape +1, github-homepage +1)
    // agreeing on domain B (+3 agreement bonus each) — B's candidates should win.
    const lonePath = candidate('https://a.com/docs', 'docs-path', true)
    const readmeOnB = candidate('https://docs.b.com/guide', 'readme-scrape', true)
    const homepageOnB = candidate('https://docs.b.com', 'github-homepage', true)
    const winner = rankCandidates([lonePath, readmeOnB, homepageOnB])
    expect(winner?.url).toMatch(/b\.com/)
  })

  test('domain agreement bonus counts distinct methods, not raw URL count on a domain', () => {
    // docs-path alone probes /docs, /documentation and /doc on one domain — that's one
    // strategy's redundant guesses, not independent corroboration, so it must not out-agree
    // a domain confirmed by a single different (and otherwise higher-scoring) strategy.
    const singleStrategyTriple = [
      candidate('https://a.com/docs', 'docs-path', true),
      candidate('https://a.com/documentation', 'docs-path', true),
      candidate('https://a.com/doc', 'docs-path', true),
    ]
    const docsSubdomainOnB = candidate('https://docs.b.com', 'docs-subdomain', true)
    const winner = rankCandidates([...singleStrategyTriple, docsSubdomainOnB])
    expect(winner).toEqual(docsSubdomainOnB)
  })

  test('domain affinity bonus: the project domain outranks an unrelated but better URL-shaped domain', () => {
    // Without domain affinity, an unrelated third-party "docs." host can outscore the
    // project's own site purely on URL shape (this mirrors a real POC case: a SERP hit for
    // an unrelated vendor's API reference outscored the project's own homepage + /docs pair).
    const ownHomepage = candidate('https://example.com', 'github-homepage', true)
    const ownDocsPage = candidate('https://example.com/docs', 'readme-scrape', true)
    const unrelatedDocs = candidate('https://docs.unrelated-vendor.com/reference', 'serp', true)

    expect(rankCandidates([ownHomepage, ownDocsPage, unrelatedDocs])).toEqual(unrelatedDocs)
    expect(rankCandidates([ownHomepage, ownDocsPage, unrelatedDocs], 'example.com')).toEqual(
      ownDocsPage,
    )
  })

  test('penalizes a bare host/path with no docs signal', () => {
    // Same method (project-website, +1) and same live status; only difference is the docs signal
    // in the URL shape, so the penalty on the bare one is what decides the winner.
    const bareMarketing = candidate('https://example.com/about', 'project-website', true)
    const docsShaped = candidate('https://example.com/documentation', 'project-website', true)
    expect(rankCandidates([bareMarketing, docsShaped])).toEqual(docsShaped)
  })

  test('is stable when every candidate scores identically (first wins)', () => {
    const first = candidate('https://a.com/docs', 'docs-path', true)
    const second = candidate('https://b.com/docs', 'docs-path', true)
    expect(rankCandidates([first, second])).toEqual(first)
  })
})

describe('rankCandidates — replay against real POC discovery outcomes', () => {
  const fixtures = pocOutcomes as Array<{
    projectSlug: string
    docsUrl: string | null
    discoveryMethod: string | null
    allCandidates: IDocCandidate[]
  }>

  // Mirrors discoverDocs's own serp gate (see index.ts): serp candidates are only kept when no
  // non-serp candidate in the fixture is live. Without this, a "match" here only proves
  // rankCandidates parity with the original POC's ranker on a candidate set discoverDocs could
  // never actually produce — not that discoverDocs itself would reach that outcome. This replay
  // still doesn't cover domain-affinity scoring, since the fixture carries no project website to
  // derive a projectDomain from; see the dedicated "domain affinity bonus" test above for that.
  function replayCandidates(allCandidates: IDocCandidate[]): IDocCandidate[] {
    const nonSerp = allCandidates.filter((c) => c.method !== 'serp')
    return nonSerp.some((c) => c.livenessOk) ? nonSerp : allCandidates
  }

  // These 3 recorded POC outcomes are serp results, but their fixture also has a live non-serp
  // candidate — under discoverDocs's serp gate, serp never runs for them, so the recorded
  // outcome is structurally unreachable by the current pipeline. discoverDocs's own (better)
  // answer is asserted instead. Flagged in review:
  // https://github.com/linuxfoundation/crowd.dev/pull/4682#discussion_r4059916800
  const GATE_UNREACHABLE: Record<string, { url: string; method: string }> = {
    'finos-community': { url: 'https://landscape.finos.org/docs', method: 'docs-path' },
    kairos: { url: 'https://kairos.io/docs', method: 'docs-path' },
    openfeature: { url: 'https://docs.openfeature.dev', method: 'docs-subdomain' },
    insights: { url: 'https://insights.linuxfoundation.org/docs', method: 'readme-scrape' },
    'ojsf-dojo': { url: 'https://dojo.io', method: 'project-website' },
    e4s: { url: 'https://e4s.io/documentation', method: 'docs-path' },
  }

  test('fixture has real, varied outcomes to replay', () => {
    expect(fixtures.length).toBeGreaterThan(20)
    const methods = new Set(fixtures.map((f) => f.discoveryMethod))
    expect(methods.size).toBeGreaterThan(4)
  })

  for (const fixture of fixtures) {
    test(`${fixture.projectSlug}: winner matches recorded POC outcome`, () => {
      const winner = rankCandidates(replayCandidates(fixture.allCandidates))
      const expected = GATE_UNREACHABLE[fixture.projectSlug] ?? {
        url: fixture.docsUrl,
        method: fixture.discoveryMethod,
      }

      if (expected.url === null) {
        expect(winner).toBeNull()
        return
      }

      expect(winner).not.toBeNull()
      expect(winner?.url).toBe(expected.url)
      expect(winner?.method).toBe(expected.method)
    })
  }
})
