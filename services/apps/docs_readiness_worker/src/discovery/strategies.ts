import { canonicalizeGithubRepoUrl } from '@crowd/common'
import type {
  DocDiscoveryConfidence,
  DocDiscoveryMethod,
  IDocCandidate,
} from '@crowd/data-access-layer'

import {
  type IRepoRef,
  getPackageJson,
  getReadme,
  getRepoHomepage,
  parseGithubRepo,
  primaryRepo,
} from './github'
import {
  USER_AGENT,
  domainOf,
  fetchText,
  isGithubWebsite,
  isLiveDocs,
  normalizeUrl,
  normalizedDomain,
} from './http'

export interface IDiscoveryContext {
  name: string
  slug: string
  website: string | null
  websiteShared: boolean
  findSharedDocsUrls?: (hosts: string[]) => Promise<string[]>
  repos: IRepoRef[]
  githubToken: string | null
  serpApiKey: string | null
}

export type DiscoveryStrategy = (ctx: IDiscoveryContext) => Promise<IDocCandidate[]>

type NonOverrideMethod = Exclude<DocDiscoveryMethod, 'override'>

export const STRATEGY_CONFIDENCE: Record<NonOverrideMethod, DocDiscoveryConfidence> = {
  'llms-txt-probe': 'high',
  'docs-subdomain': 'high',
  'docs-path': 'medium',
  'package-manifest': 'medium',
  'readme-scrape': 'medium',
  'github-homepage': 'medium',
  serp: 'low',
  'project-website': 'low',
  'repo-url': 'low',
}

export const DOCS_KEYWORDS =
  /\b(docs?|documentation|guide|reference|manual|handbook|api[\s-]?ref)\b/i

const BADGE_HOSTS = ['shields.io', 'img.shields.io', 'badge.fury.io']

function candidate(url: string, method: NonOverrideMethod, livenessOk: boolean): IDocCandidate {
  return { url, method, confidence: STRATEGY_CONFIDENCE[method], livenessOk }
}

function isBadgeOrGithubHost(host: string | null): boolean {
  if (!host) {
    return false
  }
  return (
    host === 'github.com' ||
    host === 'www.github.com' ||
    BADGE_HOSTS.some((badge) => host === badge || host.endsWith(`.${badge}`))
  )
}

// A GitHub website or one shared with other projects says nothing about this project's docs.
const usableWebsite = (ctx: IDiscoveryContext): string | null =>
  ctx.website && !ctx.websiteShared && !isGithubWebsite(ctx.website) ? ctx.website : null

const hasPath = (url: string): boolean => new URL(url).pathname !== '/'

const isLlmsTxtBody = (body: string | null): body is string =>
  !!body && body.length > 50 && !/^\s*</.test(body)

export const llmsTxtProbe = async (
  ctx: IDiscoveryContext,
  pathScopedOnly = false,
): Promise<IDocCandidate[]> => {
  const website = usableWebsite(ctx)
  if (!website) {
    return []
  }

  try {
    const normalized = normalizeUrl(website)
    const domain = normalizedDomain(website)
    if (!normalized || !domain) {
      return []
    }

    const parsed = new URL(normalized)
    parsed.search = ''
    const websiteBase = parsed.toString().replace(/\/+$/, '')
    const rootBase = `https://${domain}`

    // The website's own path first (a project page on a foundation site), root last.
    const bases = [
      ...new Set([
        ...(parsed.pathname === '/' ? [] : [websiteBase]),
        ...(pathScopedOnly ? [] : [`https://docs.${domain}`, rootBase]),
      ]),
    ]

    for (const base of bases) {
      if (isLlmsTxtBody(await fetchText(`${base}/llms.txt`, 5_000))) {
        return [candidate(base, 'llms-txt-probe', true)]
      }
    }
    return []
  } catch {
    return []
  }
}

export const docsSubdomain: DiscoveryStrategy = async (ctx) => {
  const website = usableWebsite(ctx)
  if (!website) {
    return []
  }

  try {
    const url = `https://docs.${normalizedDomain(website)}`
    return (await isLiveDocs(url)) ? [candidate(url, 'docs-subdomain', true)] : []
  } catch {
    return []
  }
}

export const docsPath: DiscoveryStrategy = async (ctx) => {
  const website = usableWebsite(ctx)
  if (!website) {
    return []
  }

  try {
    const normalized = normalizeUrl(website)
    if (!normalized) {
      return []
    }
    const parsed = new URL(normalized)
    parsed.search = ''
    const basePath = parsed.pathname.endsWith('/') ? parsed.pathname.slice(0, -1) : parsed.pathname

    const candidates: IDocCandidate[] = []
    for (const suffix of ['/docs', '/documentation', '/doc']) {
      parsed.pathname = `${basePath}${suffix}`
      const url = parsed.toString()
      if (await isLiveDocs(url)) {
        candidates.push(candidate(url, 'docs-path', true))
      }
    }
    // A catch-all site (SPA) answers 200 for every path, so a live /docs proves nothing.
    parsed.pathname = `${basePath}/__docs-readiness-soft-404-probe__`
    return candidates.length > 0 && (await isLiveDocs(parsed.toString())) ? [] : candidates
  } catch {
    return []
  }
}

export const packageManifest: DiscoveryStrategy = async (ctx) => {
  const repo = primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name })
  if (!repo || !ctx.githubToken) {
    return []
  }

  try {
    const parsed = parseGithubRepo(repo)
    if (!parsed) {
      return []
    }

    const pkg = await getPackageJson(parsed.owner, parsed.repo, ctx.githubToken)
    const raw = pkg?.documentation ?? pkg?.homepage
    if (!raw) {
      return []
    }

    const url = normalizeUrl(raw)
    if (!url) {
      return []
    }

    const repoUrl = normalizeUrl(repo)
    if (isBadgeOrGithubHost(domainOf(url)) || url === repoUrl) {
      return []
    }

    return [candidate(url, 'package-manifest', await isLiveDocs(url))]
  } catch {
    return []
  }
}

export const readmeScrape: DiscoveryStrategy = async (ctx) => {
  const repo = primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name })
  if (!repo || !ctx.githubToken) {
    return []
  }

  try {
    const parsed = parseGithubRepo(repo)
    if (!parsed) {
      return []
    }

    const readme = await getReadme(parsed.owner, parsed.repo, ctx.githubToken)
    if (!readme) {
      return []
    }

    const links = new Map<string, string>()
    for (const match of readme.matchAll(/\[([^\]]*)\]\((https?:\/\/[^)\s]+)\)/g)) {
      links.set(match[2], match[1])
    }
    for (const match of readme.matchAll(/href="(https?:\/\/[^"]+)"/g)) {
      if (!links.has(match[1])) {
        links.set(match[1], '')
      }
    }

    const filtered: string[] = []
    for (const [url, text] of links) {
      if (!DOCS_KEYWORDS.test(text) && !DOCS_KEYWORDS.test(url)) {
        continue
      }
      if (isBadgeOrGithubHost(domainOf(url))) {
        continue
      }
      filtered.push(url)
    }

    const candidates: IDocCandidate[] = []
    for (const url of filtered.slice(0, 5)) {
      if (await isLiveDocs(url)) {
        candidates.push(candidate(url, 'readme-scrape', true))
      }
    }
    return candidates
  } catch {
    return []
  }
}

export const githubHomepage: DiscoveryStrategy = async (ctx) => {
  const repo = primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name })
  if (!repo || !ctx.githubToken) {
    return []
  }

  try {
    const parsed = parseGithubRepo(repo)
    if (!parsed) {
      return []
    }

    const homepage = await getRepoHomepage(parsed.owner, parsed.repo, ctx.githubToken)
    if (!homepage) {
      return []
    }

    const url = normalizeUrl(homepage)
    if (!url) {
      return []
    }

    const repoUrl = normalizeUrl(repo)
    if (isBadgeOrGithubHost(domainOf(url)) || url === repoUrl) {
      return []
    }

    // Probe the repo homepage like a website, so a shared or GitHub project website still finds docs.
    // Skip when it is the website's own host: shared hosts stay gated, usable ones already ran.
    const homepageDomain = normalizedDomain(url)
    if (
      homepageDomain?.startsWith('docs.') ||
      (ctx.website && normalizedDomain(ctx.website) === homepageDomain)
    ) {
      return [candidate(url, 'github-homepage', await isLiveDocs(url))]
    }

    const derivedCtx: IDiscoveryContext = { ...ctx, website: url, websiteShared: false }
    // A homepage under a path (npmjs.com/package/x) is no evidence about the host's docs.
    const pathScoped = hasPath(url)
    const derived = await Promise.all([
      docsPath(derivedCtx),
      pathScoped ? [] : docsSubdomain(derivedCtx),
      llmsTxtProbe(derivedCtx, pathScoped),
    ])

    return [candidate(url, 'github-homepage', await isLiveDocs(url)), ...derived.flat()]
  } catch {
    return []
  }
}

export const projectWebsite: DiscoveryStrategy = async (ctx) => {
  const website = usableWebsite(ctx)
  if (!website) {
    return []
  }

  try {
    const url = normalizeUrl(website)
    if (!url) {
      return []
    }

    return (await isLiveDocs(url)) ? [candidate(url, 'project-website', true)] : []
  } catch {
    return []
  }
}

// Last resort for repo-only projects; ranking keeps it below every other live candidate.
export const repoUrl: DiscoveryStrategy = async (ctx) => {
  const repo = primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name })
  const url = canonicalizeGithubRepoUrl(repo)
  return url ? [candidate(url, 'repo-url', true)] : []
}

export const STRATEGIES: DiscoveryStrategy[] = [
  llmsTxtProbe,
  docsSubdomain,
  docsPath,
  packageManifest,
  readmeScrape,
  githubHomepage,
  projectWebsite,
  repoUrl,
]

export const serpStrategy: DiscoveryStrategy = async (ctx) => {
  if (!ctx.serpApiKey) {
    return []
  }

  try {
    const query = encodeURIComponent(`${ctx.name} documentation`)
    const response = await fetch(
      `https://serpapi.com/search.json?q=${query}&num=5&api_key=${ctx.serpApiKey}`,
      { signal: AbortSignal.timeout(10_000), headers: { 'User-Agent': USER_AGENT } },
    )
    if (!response.ok) {
      return []
    }

    const body = (await response.json()) as {
      organic_results?: { link?: string; title?: string }[]
    }
    const results = body.organic_results ?? []

    const kept = results.filter((result) => {
      if (!result.link) {
        return false
      }
      const host = domainOf(result.link)
      if (!host || host === 'github.com' || host === 'www.github.com') {
        return false
      }
      return (
        host.startsWith('docs.') ||
        DOCS_KEYWORDS.test(host) ||
        DOCS_KEYWORDS.test(result.link) ||
        DOCS_KEYWORDS.test(result.title ?? '')
      )
    })

    const candidates: IDocCandidate[] = []
    for (const result of kept.slice(0, 5)) {
      const url = result.link as string
      if (await isLiveDocs(url)) {
        candidates.push(candidate(url, 'serp', true))
      }
    }
    return candidates
  } catch {
    return []
  }
}
