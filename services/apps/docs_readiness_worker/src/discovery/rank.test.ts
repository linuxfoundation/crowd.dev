import { readFileSync } from 'fs'
import { join } from 'path'

import { describe, expect, test } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import { normalizedDomain } from './http'
import { candidateHasDocsSignal, rankCandidates, repoNameAnchor } from './rank'

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
    // Guards against inflating the agreement bonus by URL count instead of distinct methods:
    // one strategy probing 3 paths must not out-agree a domain confirmed by another strategy.
    const singleStrategyTriple = [
      candidate('https://a.com/docs', 'docs-path', true),
      candidate('https://a.com/documentation', 'docs-path', true),
      candidate('https://a.com/doc', 'docs-path', true),
    ]
    const docsSubdomainOnB = candidate('https://docs.b.com', 'docs-subdomain', true)
    const winner = rankCandidates([...singleStrategyTriple, docsSubdomainOnB])
    expect(winner).toEqual(docsSubdomainOnB)
  })

  test('domain affinity: a live, signal-bearing candidate on the project domain wins over an unrelated but better URL-shaped domain', () => {
    // Without domain affinity, an unrelated third-party "docs." host can outscore the
    // project's own on-domain result purely on URL shape.
    const ownHomepage = candidate('https://example.com', 'github-homepage', true)
    const ownDocsPage = candidate('https://example.com/docs', 'readme-scrape', true)
    const unrelatedDocs = candidate('https://docs.unrelated-vendor.com/reference', 'serp', true)

    expect(rankCandidates([ownHomepage, ownDocsPage, unrelatedDocs])).toEqual(unrelatedDocs)
    expect(rankCandidates([ownHomepage, ownDocsPage, unrelatedDocs], 'example.com')).toEqual(
      ownDocsPage,
    )
  })

  test('domain affinity: an on-domain signal-bearing hit wins even without a domain-agreement assist', () => {
    // A lone same-method on-domain hit has no other candidate to earn an agreement bonus from,
    // so affinity must gate to the project domain rather than rely on outscoring off-domain shape.
    const ownDocsHit = candidate('https://example.com/docs', 'serp', true)
    const unrelatedDocs = candidate('https://docs.unrelated-vendor.com/reference', 'serp', true)
    expect(rankCandidates([ownDocsHit, unrelatedDocs], 'example.com')).toEqual(ownDocsHit)
  })

  test('domain affinity: a signal-less live candidate on the project domain does not shadow real off-domain docs', () => {
    const bareHomepage = candidate('https://example.com', 'project-website', true)
    const offDomainDocs = candidate('https://docs.other.com/guide', 'serp', true)
    expect(rankCandidates([bareHomepage, offDomainDocs], 'example.com')).toEqual(offDomainDocs)
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

  test('projectNameHint (third arg) narrows the pool to hosts containing the name token', () => {
    const ownRepo = candidate('https://acme-widgets.io/docs', 'serp', true)
    const unrelated = candidate(
      'https://docs.unrelated-vendor.com/reference',
      'llms-txt-probe',
      true,
    )
    expect(rankCandidates([unrelated, ownRepo], null, 'acme-widgets')).toEqual(ownRepo)
  })

  test('a short/generic projectNameHint does not narrow the pool by substring match', () => {
    // 'ai' would coincidentally match 'ai-widgets.io' too, so a token this short/generic
    // must be ignored rather than used as a domain anchor.
    const coincidentalMatch = candidate('https://ai-widgets.io/docs', 'serp', true)
    const realDocs = candidate('https://docs.realproject.dev', 'llms-txt-probe', true)
    expect(rankCandidates([coincidentalMatch, realDocs], null, 'ai')).toEqual(realDocs)
  })

  test('a bare github.com candidate never outranks a real signal-bearing candidate, even with a strong method and multiple agreeing strategies', () => {
    const probe = candidate('https://github.com/a', 'llms-txt-probe', true)
    const subdomain = candidate('https://github.com/b', 'docs-subdomain', true)
    const homepage = candidate('https://github.com/c', 'project-website', true)
    const realDocs = candidate('https://docs.realproject.dev', 'serp', true)
    expect(rankCandidates([probe, subdomain, homepage, realDocs])).toEqual(realDocs)
  })
})

describe('rankCandidates — replay against real POC discovery outcomes', () => {
  const fixtures = pocOutcomes as Array<{
    projectSlug: string
    docsUrl: string | null
    discoveryMethod: string | null
    allCandidates: IDocCandidate[]
  }>

  // Mirrors discoverDocs's own serp gate (index.ts) so a "match" here proves parity with what
  // discoverDocs would actually produce, not just with rankCandidates on an unreachable input.
  function replayCandidates(allCandidates: IDocCandidate[]): IDocCandidate[] {
    const nonSerp = allCandidates.filter((c) => c.method !== 'serp')
    return nonSerp.some((c) => c.livenessOk && candidateHasDocsSignal(c)) ? nonSerp : allCandidates
  }

  // Mirrors discoverDocs's own projectDomain derivation (ctx.website), using the fixture's
  // project-website candidate as the stand-in for ctx.website since the POC didn't record it.
  function replayProjectDomain(allCandidates: IDocCandidate[]): string | null {
    const website = allCandidates.find((c) => c.method === 'project-website')
    const domain = website ? normalizedDomain(website.url) : null
    return domain === 'github.com' ? null : domain
  }

  // Mirrors discoverDocs's own name-anchor fallback for a github.com project website.
  function replayProjectNameHint(allCandidates: IDocCandidate[]): string | null {
    const website = allCandidates.find((c) => c.method === 'project-website')
    if (!website || normalizedDomain(website.url) !== 'github.com') {
      return null
    }
    return repoNameAnchor(website.url)
  }

  // Recorded POC outcomes for these are serp results, but a live signal-bearing candidate
  // already blocks serp from running — asserts the gate-reachable answer instead.
  const GATE_UNREACHABLE: Record<string, { url: string; method: string }> = {
    'finos-community': { url: 'https://landscape.finos.org/docs', method: 'docs-path' },
    kairos: { url: 'https://kairos.io/docs', method: 'docs-path' },
    openfeature: { url: 'https://docs.openfeature.dev', method: 'docs-subdomain' },
    insights: { url: 'https://insights.linuxfoundation.org/docs', method: 'readme-scrape' },
    e4s: { url: 'https://e4s.io/documentation', method: 'docs-path' },
  }

  // The POC predates domain affinity: its recorded winner is an off-domain result even though
  // an on-domain, signal-bearing candidate exists — asserts the affinity-corrected answer instead.
  const AFFINITY_ADJUSTED: Record<string, { url: string; method: string }> = {
    cobaltcore: { url: 'https://cobaltcore-dev.github.io/docs', method: 'project-website' },
  }

  // The POC recorded GitHub's own shared host (docs.github.com/github.com) as the winner; it no
  // longer counts as a docs signal, so these assert the corrected answer instead.
  const GITHUB_HOST_SUPPRESSED: Record<string, { url: string; method: string }> = {
    lima: { url: 'https://lima-vm.io/docs/', method: 'serp' },
    'open-resource-discovery': {
      url: 'https://ord-reference-application.cfapps.sap.hana.ondemand.com/',
      method: 'serp',
    },
    'ai-governance-framework': {
      url: 'https://www.linkedin.com/pulse/ai-governance-documentation-practical-framework-business-derek-martin-sbvfe',
      method: 'serp',
    },
  }

  test('fixture has real, varied outcomes to replay', () => {
    expect(fixtures.length).toBeGreaterThan(20)
    const methods = new Set(fixtures.map((f) => f.discoveryMethod))
    expect(methods.size).toBeGreaterThan(4)
  })

  for (const fixture of fixtures) {
    const gateAdjusted = GATE_UNREACHABLE[fixture.projectSlug]
    const affinityAdjusted = AFFINITY_ADJUSTED[fixture.projectSlug]
    const githubHostSuppressed = GITHUB_HOST_SUPPRESSED[fixture.projectSlug]
    const testName = gateAdjusted
      ? `${fixture.projectSlug}: winner matches gate-adjusted outcome (recorded serp result is unreachable)`
      : affinityAdjusted
        ? `${fixture.projectSlug}: winner matches affinity-adjusted outcome (recorded outcome predates domain affinity)`
        : githubHostSuppressed
          ? `${fixture.projectSlug}: winner matches corrected outcome (recorded winner was GitHub's shared host)`
          : `${fixture.projectSlug}: winner matches recorded POC outcome`

    test(testName, () => {
      const winner = rankCandidates(
        replayCandidates(fixture.allCandidates),
        replayProjectDomain(fixture.allCandidates),
        replayProjectNameHint(fixture.allCandidates),
      )
      const expected = gateAdjusted ??
        affinityAdjusted ??
        githubHostSuppressed ?? {
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
