import type { IDocCandidate } from '@crowd/data-access-layer'

import { domainOf, normalizedDomain } from './http'
import { DOCS_KEYWORDS } from './strategies'

const METHOD_BONUS: Partial<Record<IDocCandidate['method'], number>> = {
  'llms-txt-probe': 6,
  'docs-subdomain': 4,
  'docs-path': 3,
  serp: 2,
  'package-manifest': 2,
  'readme-scrape': 1,
  'github-homepage': 1,
  'project-website': 1,
}

function pathnameOf(url: string): string {
  try {
    return new URL(url).pathname
  } catch {
    return ''
  }
}

function hasDocsSignal(host: string, pathname: string): boolean {
  return host.startsWith('docs.') || DOCS_KEYWORDS.test(host) || DOCS_KEYWORDS.test(pathname)
}

export function candidateHasDocsSignal(c: IDocCandidate): boolean {
  return hasDocsSignal(domainOf(c.url) ?? '', pathnameOf(c.url))
}

function isOnProjectDomain(url: string, projectDomain: string): boolean {
  const domain = normalizedDomain(url)
  return domain === projectDomain || (domain?.endsWith(`.${projectDomain}`) ?? false)
}

export function rankCandidates(
  candidates: IDocCandidate[],
  projectDomain: string | null = null,
): IDocCandidate | null {
  const live = candidates.filter((c) => c.livenessOk)
  if (live.length === 0) {
    return null
  }

  // A live candidate on the project's own domain only overrides off-domain results once it
  // actually carries a docs signal — otherwise a bare homepage would shadow real off-domain docs.
  const ownDomainLive = projectDomain
    ? live.filter((c) => isOnProjectDomain(c.url, projectDomain))
    : []
  const pool = ownDomainLive.some(candidateHasDocsSignal) ? ownDomainLive : live

  const domainMethods: Record<string, Set<IDocCandidate['method']>> = {}
  for (const c of pool) {
    const domain = normalizedDomain(c.url)
    if (!domainMethods[domain]) {
      domainMethods[domain] = new Set()
    }
    domainMethods[domain].add(c.method)
  }

  const scored = pool.map((c) => {
    const host = domainOf(c.url) ?? ''
    const pathname = pathnameOf(c.url)
    const domain = normalizedDomain(c.url)

    let score = METHOD_BONUS[c.method] ?? 0

    if (host.startsWith('docs.')) score += 4
    if (DOCS_KEYWORDS.test(host)) score += 2
    if (DOCS_KEYWORDS.test(pathname)) score += 2

    score += (domainMethods[domain].size - 1) * 3

    if (!hasDocsSignal(host, pathname)) {
      score -= 2
    }

    return { candidate: c, score }
  })

  scored.sort((a, b) => b.score - a.score)
  return scored[0].candidate
}
