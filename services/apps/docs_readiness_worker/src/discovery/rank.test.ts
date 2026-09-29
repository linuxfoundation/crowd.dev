import { readFileSync } from 'fs'
import { join } from 'path'

import { describe, expect, test } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import { normalizedDomain } from './http'
import { isOrganicCandidate, rankCandidates, repoNameAnchor } from './rank'

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
    const manifest = candidate('https://c.com', 'package-manifest', true)
    const readme = candidate('https://d.com', 'readme-scrape', true)
    expect(rankCandidates([path, subdomain])).toEqual(subdomain)
    expect(rankCandidates([manifest, path])).toEqual(path)
    expect(rankCandidates([readme, manifest])).toEqual(manifest)
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
    const unrelatedDocs = candidate(
      'https://docs.unrelated-vendor.com/reference',
      'package-manifest',
      true,
    )

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
    const offDomainDocs = candidate('https://docs.other.com/guide', 'package-manifest', true)
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
    const ownRepo = candidate('https://acme-widgets.io/docs', 'package-manifest', true)
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
    const realDocs = candidate('https://docs.realproject.dev', 'package-manifest', true)
    expect(rankCandidates([probe, subdomain, homepage, realDocs])).toEqual(realDocs)
  })
})

describe('rankCandidates — root-domain demotion (IN-1393)', () => {
  test('a bare foundation llms.txt root loses to a project page on the same domain (report case 10)', () => {
    const root = candidate('https://openmainframeproject.org', 'llms-txt-probe', true)
    const project = candidate(
      'https://openmainframeproject.org/projects/cobol-programming-course',
      'github-homepage',
      true,
    )
    expect(rankCandidates([root, project])).toEqual(project)
    expect(rankCandidates([project, root])).toEqual(project)
    expect(rankCandidates([root, project], 'openmainframeproject.org')).toEqual(project)
  })

  test('a docs.-prefixed llms.txt hit keeps its full bonus over a bare project website', () => {
    const docs = candidate('https://docs.example.com', 'llms-txt-probe', true)
    const site = candidate('https://example.com/projects/x', 'github-homepage', true)
    expect(rankCandidates([site, docs])).toEqual(docs)
  })

  test('a path-preserving llms.txt hit keeps its full bonus', () => {
    const llms = candidate('https://foundation.org/projects/x', 'llms-txt-probe', true)
    const site = candidate('https://foundation.org/projects/x/about', 'github-homepage', true)
    expect(rankCandidates([site, llms])).toEqual(llms)
  })

  test('a bare llms.txt root is still returned when it is the only live candidate', () => {
    const root = candidate('https://example.com', 'llms-txt-probe', true)
    expect(rankCandidates([root])).toEqual(root)
  })
})

const sharedSet = (...urls: string[]) => new Set(urls)

describe('rankCandidates — shared docs URL penalty (IN-1393)', () => {
  test('an unshared candidate beats a shared one even when the shared one has the stronger method', () => {
    const shared = candidate('https://docs.lfenergy.org', 'docs-subdomain', true)
    const own = candidate('https://myproject.org', 'project-website', true)
    expect(
      rankCandidates([shared, own], null, null, sharedSet('https://docs.lfenergy.org')),
    ).toEqual(own)
  })

  test('the penalty matches the same site across scheme, www, query, trailing slash and host case', () => {
    const shared = candidate('https://Docs.LFEnergy.org/', 'docs-subdomain', true)
    const own = candidate('https://myproject.org', 'project-website', true)
    expect(
      rankCandidates([shared, own], null, null, sharedSet('https://docs.lfenergy.org/')),
    ).toEqual(own)
  })

  test.each([
    'http://docs.lfenergy.org/x',
    'https://www.docs.lfenergy.org/x',
    'https://docs.lfenergy.org/x?v=1',
    'https://docs.lfenergy.org/x/',
  ])('a stored shared URL %s still penalises https://docs.lfenergy.org/x', (stored) => {
    const shared = candidate('https://docs.lfenergy.org/x', 'docs-subdomain', true)
    const own = candidate('https://myproject.org', 'project-website', true)
    expect(rankCandidates([shared, own], null, null, sharedSet(stored))).toEqual(own)
  })

  test('path case matters: a shared /ProjectA penalises /ProjectA but not /projecta', () => {
    const upper = candidate('https://docs.foundation.org/ProjectA', 'docs-subdomain', true)
    const lower = candidate('https://docs.foundation.org/projecta', 'docs-subdomain', true)
    const own = candidate('https://myproject.org', 'project-website', true)
    const set = sharedSet('https://docs.foundation.org/ProjectA')
    expect(rankCandidates([upper, own], null, null, set)).toEqual(own)
    expect(rankCandidates([lower, own], null, null, set)).toEqual(lower)
  })

  test('a different path on the same host is not penalised', () => {
    const other = candidate('https://docs.lfenergy.org/y', 'docs-subdomain', true)
    const own = candidate('https://myproject.org', 'project-website', true)
    expect(
      rankCandidates([other, own], null, null, sharedSet('https://docs.lfenergy.org/x')),
    ).toEqual(other)
  })

  test('a shared URL still beats bare GitHub hosts', () => {
    const shared = candidate('https://foundation.org', 'project-website', true)
    const github = candidate('https://github.com', 'project-website', true)
    expect(
      rankCandidates([github, shared], null, null, sharedSet('https://foundation.org')),
    ).toEqual(shared)
  })

  test('an unshared repo-url beats a shared URL (AC3, open-reg-tech-us-lcr shape)', () => {
    const shared = candidate('https://community.finos.org/docs/easycla', 'readme-scrape', true)
    const repo = candidate('https://github.com/finos/open-reg-tech-us-lcr', 'repo-url', true, 'low')
    const set = sharedSet('https://community.finos.org/docs/easycla')
    expect(rankCandidates([shared, repo], null, null, set)).toEqual(repo)
    expect(rankCandidates([repo, shared], null, null, set)).toEqual(repo)
  })

  test('an unshared repo-url beats a shared repo-url, in either order', () => {
    const shared = candidate('https://github.com/org/shared-repo', 'repo-url', true, 'low')
    const own = candidate('https://github.com/org/own-repo', 'repo-url', true, 'low')
    const set = sharedSet('https://github.com/org/shared-repo')
    expect(rankCandidates([shared, own], null, null, set)).toEqual(own)
    expect(rankCandidates([own, shared], null, null, set)).toEqual(own)
  })

  test('an unshared repo-url beats the strongest possible shared candidate', () => {
    const url = 'https://docs.big.org/docs'
    const methods = [
      'llms-txt-probe',
      'docs-subdomain',
      'docs-path',
      'serp',
      'package-manifest',
      'readme-scrape',
      'github-homepage',
      'project-website',
    ] as const
    const shared = methods.map((m) => candidate(url, m, true))
    const repo = candidate('https://github.com/o/r', 'repo-url', true, 'low')
    expect(rankCandidates([...shared, repo], null, null, sharedSet(url))).toEqual(repo)
  })

  test('a shared root on the project domain does not hide an unshared repo-url (ade shape)', () => {
    const root = candidate('https://openmainframeproject.org', 'llms-txt-probe', true)
    const repo = candidate('https://github.com/openmainframeproject/ade', 'repo-url', true, 'low')
    const set = sharedSet('https://openmainframeproject.org')
    expect(rankCandidates([root, repo], 'openmainframeproject.org', null, set)).toEqual(repo)
    expect(rankCandidates([root, repo], 'openmainframeproject.org')).toEqual(root)
  })

  test('an unshared on-domain docs candidate still anchors the pool over off-domain ones', () => {
    const own = candidate('https://docs.proj.org', 'docs-subdomain', true)
    const other = candidate('https://good.io/docs', 'serp', true)
    const set = sharedSet('https://unrelated.org')
    expect(rankCandidates([other, own], 'proj.org', null, set)).toEqual(own)
  })

  test('an unshared repo-url still loses to a weak unshared non-GitHub candidate', () => {
    const weak = candidate('https://example.com', 'project-website', true)
    const repo = candidate('https://github.com/o/r', 'repo-url', true, 'low')
    const set = sharedSet('https://other.example.org')
    expect(rankCandidates([repo, weak], null, null, set)).toEqual(weak)
  })

  test('a shared URL is the winner when it is the only live candidate', () => {
    const shared = candidate('https://foundation.org', 'project-website', true)
    expect(rankCandidates([shared], null, null, sharedSet('https://foundation.org'))).toEqual(
      shared,
    )
  })

  test('no shared set leaves ranking unchanged', () => {
    const shared = candidate('https://docs.lfenergy.org', 'docs-subdomain', true)
    const own = candidate('https://myproject.org', 'project-website', true)
    expect(rankCandidates([own, shared])).toEqual(shared)
  })
})

describe('rankCandidates — GitHub tie-break and repo-url (IN-1393)', () => {
  test('a github.com/<org>/<repo> candidate beats docs.github.com (report case 19)', () => {
    const docsGithub = candidate('https://docs.github.com', 'llms-txt-probe', true)
    const repo = candidate('https://github.com/piraeusdatastore/docs', 'project-website', true)
    expect(rankCandidates([docsGithub, repo])).toEqual(repo)
    expect(rankCandidates([repo, docsGithub])).toEqual(repo)
  })

  test('a github.com repo path beats bare github.com', () => {
    const bare = candidate('https://github.com', 'project-website', true)
    const repo = candidate('https://github.com/org/repo', 'readme-scrape', true)
    expect(rankCandidates([bare, repo])).toEqual(repo)
  })

  test('repo-url is chosen when it is the only live candidate', () => {
    const repo = candidate('https://github.com/org/repo', 'repo-url', true, 'low')
    expect(rankCandidates([repo])).toEqual(repo)
  })

  test('repo-url loses to any non-GitHub live candidate, however weak', () => {
    const repo = candidate('https://github.com/org/repo', 'repo-url', true, 'low')
    const weak = candidate('https://example.com', 'project-website', true)
    expect(rankCandidates([repo, weak])).toEqual(weak)
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
    return nonSerp.some((c) => c.livenessOk && isOrganicCandidate(c)) ? nonSerp : allCandidates
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
  }

  // The POC picked a SERP guess over the project's own homepage; SERP never outranks a live organic candidate.
  const SERP_DEMOTED: Record<string, { url: string; method: string }> = {
    'ojsf-dojo': { url: 'https://dojo.io', method: 'project-website' },
    'open-resource-discovery': {
      url: 'https://open-resource-discovery.org',
      method: 'project-website',
    },
    'ai-governance-framework': {
      url: 'https://air-governance-framework.finos.org',
      method: 'project-website',
    },
  }

  // POC winners that were a bare foundation llms.txt root; a project page or same-site root wins now.
  const FOUNDATION_ROOT_DEMOTED: Record<string, { url: string; method: string }> = {
    'project-eve': { url: 'https://www.lfedge.org', method: 'project-website' },
    compas: { url: 'https://www.lfenergy.org/projects/compas', method: 'project-website' },
    everest: { url: 'https://lfenergy.org/projects/everest', method: 'project-website' },
    feilong: {
      url: 'https://www.openmainframeproject.org/projects/feilong',
      method: 'github-homepage',
    },
    materialx: { url: 'https://www.aswf.io', method: 'project-website' },
    o3de: { url: 'http://o3d.foundation', method: 'project-website' },
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
    const rootDemoted = FOUNDATION_ROOT_DEMOTED[fixture.projectSlug]
    const serpDemoted = SERP_DEMOTED[fixture.projectSlug]
    const testName = gateAdjusted
      ? `${fixture.projectSlug}: winner matches gate-adjusted outcome (recorded serp result is unreachable)`
      : affinityAdjusted
        ? `${fixture.projectSlug}: winner matches affinity-adjusted outcome (recorded outcome predates domain affinity)`
        : githubHostSuppressed
          ? `${fixture.projectSlug}: winner matches corrected outcome (recorded winner was GitHub's shared host)`
          : rootDemoted
            ? `${fixture.projectSlug}: winner matches root-demoted outcome (recorded winner was a bare foundation llms.txt root)`
            : serpDemoted
              ? `${fixture.projectSlug}: winner matches serp-demoted outcome (recorded winner was a SERP guess over a live homepage)`
              : `${fixture.projectSlug}: winner matches recorded POC outcome`

    test(testName, () => {
      const winner = rankCandidates(
        replayCandidates(fixture.allCandidates),
        replayProjectDomain(fixture.allCandidates),
        replayProjectNameHint(fixture.allCandidates),
      )
      const expected = gateAdjusted ??
        affinityAdjusted ??
        githubHostSuppressed ??
        rootDemoted ??
        serpDemoted ?? {
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

describe('rankCandidates — SERP tier (IN-1396)', () => {
  test('a SERP hit with every ranking bonus never outranks a live bare homepage (5 Spot)', () => {
    const homepage = candidate('https://5spot.finos.org/', 'github-homepage', true)
    const serp = candidate('https://docs.buildbot.net/manual/schedulers.html', 'serp', true)
    expect(rankCandidates([serp, homepage])).toEqual(homepage)
    expect(rankCandidates([homepage, serp])).toEqual(homepage)
  })

  test('a SERP hit does not outrank a homepage that only carries a shared-URL penalty', () => {
    const homepage = candidate('https://example.com', 'project-website', true)
    const serp = candidate('https://docs.other.com/guide', 'serp', true)
    expect(rankCandidates([serp, homepage], null, null, new Set(['https://example.com']))).toEqual(
      homepage,
    )
  })

  test.each([
    ['the repo-url fallback', candidate('https://github.com/acme/proj', 'repo-url', true)],
    ['a GitHub shared host', candidate('https://docs.github.com/en/x', 'readme-scrape', true)],
  ])('a SERP hit still beats %s', (_name, fallback) => {
    const serp = candidate('https://proj.readthedocs.io/en/latest', 'serp', true)
    expect(rankCandidates([fallback, serp])).toEqual(serp)
  })

  test('SERP hits still rank among themselves when nothing organic is live', () => {
    const best = candidate('https://docs.proj.dev/guide', 'serp', true)
    const worse = candidate('https://proj.dev/blog', 'serp', true)
    expect(rankCandidates([worse, best])).toEqual(best)
  })

  test('a dead organic candidate does not block SERP', () => {
    const dead = candidate('https://example.com', 'project-website', false)
    const serp = candidate('https://docs.proj.dev', 'serp', true)
    expect(rankCandidates([dead, serp])).toEqual(serp)
  })
})

describe('rankCandidates — docs subdomain over the bare root (IN-1396)', () => {
  // Five methods agreeing on the root give it a +12 agreement bonus, outscoring docs.vllm.ai alone.
  const rootStack = [
    candidate('https://vllm.ai', 'llms-txt-probe', true),
    candidate('https://vllm.ai/', 'project-website', true),
    candidate('https://vllm.ai/', 'github-homepage', true),
    candidate('https://vllm.ai/', 'readme-scrape', true),
    candidate('https://vllm.ai/', 'package-manifest', true),
  ]
  const docs = candidate('https://docs.vllm.ai', 'docs-subdomain', true)

  test('vLLM: a live docs. subdomain beats the llms.txt root even when the root outscores it', () => {
    const scoredAlone = rankCandidates([
      ...rootStack,
      candidate('https://x.dev/docs', 'docs-path', true),
    ])
    expect(scoredAlone?.url).toBe('https://vllm.ai/')

    expect(rankCandidates([...rootStack, docs], 'vllm.ai')).toEqual(docs)
    expect(rankCandidates([docs, ...rootStack], 'vllm.ai')).toEqual(docs)
  })

  test('a docs. host found by another method still displaces the root', () => {
    const docsLlms = candidate('https://docs.vllm.ai', 'llms-txt-probe', true)
    expect(rankCandidates([...rootStack, docsLlms], 'vllm.ai')).toEqual(docsLlms)
  })

  test('the www root of the same registrable domain loses too', () => {
    const wwwRoot = candidate('https://www.zowe.org/', 'project-website', true)
    const stack = [
      wwwRoot,
      candidate('https://zowe.org', 'llms-txt-probe', true),
      candidate('https://zowe.org', 'github-homepage', true),
      candidate('https://zowe.org', 'package-manifest', true),
      candidate('https://zowe.org', 'readme-scrape', true),
    ]
    const zoweDocs = candidate('https://docs.zowe.org', 'docs-subdomain', true)
    expect(rankCandidates([...stack, zoweDocs])).toEqual(zoweDocs)
  })

  test('a docs. subdomain of an unrelated domain does not displace the root', () => {
    const other = candidate('https://docs.other-vendor.com', 'docs-subdomain', true)
    expect(rankCandidates([...rootStack, other])?.url).toBe('https://vllm.ai/')
  })

  test('a root with a path is not a bare root, so the score decides', () => {
    const projectPage = candidate('https://vllm.ai/projects/vllm', 'llms-txt-probe', true)
    const stack = rootStack.map((c) => ({ ...c, url: 'https://vllm.ai/projects/vllm' }))
    expect(rankCandidates([projectPage, ...stack, docs])?.url).toBe('https://vllm.ai/projects/vllm')
  })

  test('a docs. subdomain claimed by other projects does not displace the root', () => {
    const shared = new Set(['https://docs.vllm.ai'])
    expect(rankCandidates([...rootStack, docs], null, null, shared)?.url).toBe('https://vllm.ai/')
  })

  test('a dead docs. subdomain is ignored', () => {
    const dead = candidate('https://docs.vllm.ai', 'docs-subdomain', false)
    expect(rankCandidates([...rootStack, dead])?.url).toBe('https://vllm.ai/')
  })
})
