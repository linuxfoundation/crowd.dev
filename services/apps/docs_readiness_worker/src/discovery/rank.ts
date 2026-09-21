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

// github.com/www.github.com/docs.github.com are GitHub's own shared hosts, not project-specific
// documentation — unlike *.github.io, they must never count as a docs signal for any project.
const GITHUB_SHARED_HOSTS = new Set(['github.com', 'www.github.com', 'docs.github.com'])

function hasDocsSignal(host: string, pathname: string): boolean {
  if (GITHUB_SHARED_HOSTS.has(host)) {
    return false
  }
  return host.startsWith('docs.') || DOCS_KEYWORDS.test(host) || DOCS_KEYWORDS.test(pathname)
}

// These methods probe for documentation directly, so they're a signal on their own —
// a root-level llms.txt hit shouldn't need docs.*/keyword URL shape to count as one.
const EXPLICIT_DOCS_PROBE_METHODS = new Set<IDocCandidate['method']>([
  'llms-txt-probe',
  'docs-subdomain',
  'docs-path',
])

export function candidateHasDocsSignal(c: IDocCandidate): boolean {
  const host = domainOf(c.url) ?? ''
  if (GITHUB_SHARED_HOSTS.has(host)) {
    return false
  }
  return EXPLICIT_DOCS_PROBE_METHODS.has(c.method) || hasDocsSignal(host, pathnameOf(c.url))
}

function isOnProjectDomain(url: string, projectDomain: string): boolean {
  const domain = normalizedDomain(url)
  return domain === projectDomain || (domain?.endsWith(`.${projectDomain}`) ?? false)
}

// When a project's website is its own GitHub repo, projectDomain has no usable anchor — derive
// one from the repo owner so ranking isn't left fully unanchored against same-keyword domains.
export function repoNameAnchor(url: string): string | null {
  try {
    return new URL(url).pathname.split('/').filter(Boolean)[0] ?? null
  } catch {
    return null
  }
}

export function rankCandidates(
  candidates: IDocCandidate[],
  projectDomain: string | null = null,
  projectNameHint: string | null = null,
): IDocCandidate | null {
  const live = candidates.filter((c) => c.livenessOk)
  if (live.length === 0) {
    return null
  }

  const nameToken = projectNameHint ? projectNameHint.toLowerCase() : ''

  // A live candidate on the project's own domain only overrides off-domain results once it
  // actually carries a docs signal — otherwise a bare homepage would shadow real off-domain docs.
  // Lacking a real domain, fall back to a looser name-token match on the host as the anchor.
  const ownDomainLive = projectDomain
    ? live.filter((c) => isOnProjectDomain(c.url, projectDomain))
    : nameToken
      ? live.filter((c) => (normalizedDomain(c.url) ?? '').toLowerCase().includes(nameToken))
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

    if (!GITHUB_SHARED_HOSTS.has(host)) {
      if (host.startsWith('docs.')) score += 4
      if (DOCS_KEYWORDS.test(host)) score += 2
    }
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
