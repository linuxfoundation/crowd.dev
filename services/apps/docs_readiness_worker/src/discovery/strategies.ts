import type {
  DocDiscoveryConfidence,
  DocDiscoveryMethod,
  IDocCandidate,
} from '@crowd/data-access-layer'

import { getPackageJson, getReadme, getRepoHomepage, parseGithubRepo, primaryRepo } from './github'
import { USER_AGENT, domainOf, fetchText, isLiveDocs, normalizeUrl, normalizedDomain } from './http'

export interface IDiscoveryContext {
  name: string
  website: string | null
  repos: string[]
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
    BADGE_HOSTS.some((badge) => host === badge || host.endsWith(`.${badge}`))
  )
}

export const llmsTxtProbe: DiscoveryStrategy = async (ctx) => {
  if (!ctx.website) {
    return []
  }

  try {
    const domain = normalizedDomain(ctx.website)
    let body = await fetchText(`https://docs.${domain}/llms.txt`, 5_000)
    let host = `https://docs.${domain}`
    if (!body) {
      body = await fetchText(`https://${domain}/llms.txt`, 5_000)
      host = `https://${domain}`
    }

    if (!body || body.length <= 50 || /^\s*</.test(body)) {
      return []
    }

    return [candidate(host, 'llms-txt-probe', true)]
  } catch {
    return []
  }
}

export const docsSubdomain: DiscoveryStrategy = async (ctx) => {
  if (!ctx.website) {
    return []
  }

  try {
    const url = `https://docs.${normalizedDomain(ctx.website)}`
    return (await isLiveDocs(url)) ? [candidate(url, 'docs-subdomain', true)] : []
  } catch {
    return []
  }
}

export const docsPath: DiscoveryStrategy = async (ctx) => {
  if (!ctx.website) {
    return []
  }

  try {
    const normalized = normalizeUrl(ctx.website)
    if (!normalized) {
      return []
    }
    const base = normalized.endsWith('/') ? normalized.slice(0, -1) : normalized

    const candidates: IDocCandidate[] = []
    for (const suffix of ['/docs', '/documentation', '/doc']) {
      const url = `${base}${suffix}`
      if (await isLiveDocs(url)) {
        candidates.push(candidate(url, 'docs-path', true))
      }
    }
    return candidates
  } catch {
    return []
  }
}

export const packageManifest: DiscoveryStrategy = async (ctx) => {
  const repo = primaryRepo(ctx.repos)
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
  const repo = primaryRepo(ctx.repos)
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

    // README links can be markdown [text](url) or raw HTML href="url"
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
  const repo = primaryRepo(ctx.repos)
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

    return [candidate(url, 'github-homepage', await isLiveDocs(url))]
  } catch {
    return []
  }
}

export const projectWebsite: DiscoveryStrategy = async (ctx) => {
  if (!ctx.website) {
    return []
  }

  try {
    const url = normalizeUrl(ctx.website)
    if (!url) {
      return []
    }

    return (await isLiveDocs(url)) ? [candidate(url, 'project-website', true)] : []
  } catch {
    return []
  }
}

export const STRATEGIES: DiscoveryStrategy[] = [
  llmsTxtProbe,
  docsSubdomain,
  docsPath,
  packageManifest,
  readmeScrape,
  githubHomepage,
  projectWebsite,
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

    // A docs subdomain or a docs-shaped host/path/title counts as a documentation result
    const kept = results.filter((result) => {
      if (!result.link) {
        return false
      }
      const host = domainOf(result.link)
      if (!host || host === 'github.com') {
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
