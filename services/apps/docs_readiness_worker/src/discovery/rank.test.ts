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

  test('fixture has real, varied outcomes to replay', () => {
    expect(fixtures.length).toBeGreaterThan(20)
    const methods = new Set(fixtures.map((f) => f.discoveryMethod))
    expect(methods.size).toBeGreaterThan(4)
  })

  for (const fixture of fixtures) {
    test(`${fixture.projectSlug}: winner matches recorded POC outcome`, () => {
      const winner = rankCandidates(fixture.allCandidates)

      if (fixture.docsUrl === null) {
        expect(winner).toBeNull()
        return
      }

      expect(winner).not.toBeNull()
      expect(winner?.url).toBe(fixture.docsUrl)
      expect(winner?.method).toBe(fixture.discoveryMethod)
    })
  }
})
