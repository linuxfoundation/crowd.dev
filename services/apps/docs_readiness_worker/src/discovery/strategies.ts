import { canonicalizeGithubRepoUrl, registrableDomain } from '@crowd/common'
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
import { isRelevantSerpResult, projectTokens } from './relevance'

export interface IDiscoveryContext {
  name: string
  slug: string
  website: string | null
  websiteShared: boolean
  // Website shared only with twins or a family of this project, so docs URLs on it are legitimately shared.
  websiteSharedByFamily?: boolean
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

const README_EXCLUDED_HOSTS = [
  ...BADGE_HOSTS,
  'github.com',
  'gitter.im',
  'discord.gg',
  'twitter.com',
  'x.com',
  'slack.com',
  'youtube.com',
  'youtu.be',
  'githubusercontent.com',
  'githubassets.com',
  'docs.google.com',
  'drive.google.com',
]

const EXCLUDED_PATH_SEGMENT =
  /^(contribut\w*|code[-_]of[-_]conduct|security|issues?|bugs?|report|reporting[-_]bugs?|changelog|licen[sc]e)([-_.]|$)/i

function isReadmeExcludedHost(host: string | null): boolean {
  return (
    !host ||
    README_EXCLUDED_HOSTS.some((excluded) => host === excluded || host.endsWith(`.${excluded}`))
  )
}

function pathSegments(url: string): string[] {
  return new URL(url).pathname.split('/').filter(Boolean)
}

function collapseOwnDomainLink(url: string): string {
  const parsed = new URL(url)
  const segments = pathSegments(url)
  if (segments.length < 1 || parsed.hostname.endsWith('.github.io')) {
    return url
  }
  const docsIndex = segments.findIndex((segment) => DOCS_KEYWORDS.test(segment))
  const kept = docsIndex === -1 ? [] : segments.slice(0, docsIndex + 1)
  if (kept.length === segments.length) {
    return url
  }
  parsed.search = ''
  parsed.pathname = `/${kept.join('/')}`
  return normalizeUrl(parsed.toString()) ?? url
}

const GENERIC_SUBDOMAINS = new Set(['www', 'wiki', 'web'])

// Roots whose subdomains are unrelated projects (celf lives on wiki.linuxfoundation.org).
const UMBRELLA_ROOTS = new Set(['linuxfoundation.org'])

function docsBaseDomain(website: string): string | null {
  const host = normalizedDomain(website)
  const root = registrableDomain(website)
  if (!host || !root || host === root || UMBRELLA_ROOTS.has(root)) {
    return host
  }
  const subdomain = host.slice(0, -root.length - 1)
  return subdomain.split('.').every((label) => GENERIC_SUBDOMAINS.has(label)) ? root : host
}

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

const PACKAGE_REGISTRIES = [
  'npmjs.com',
  'pypi.org',
  'crates.io',
  'rubygems.org',
  'pkg.go.dev',
  'hub.docker.com',
  'packagist.org',
  'nuget.org',
]

// A path homepage (npmjs.com/package/x) or a shared host does not make the whole domain its own.
function usableHomepage(ctx: IDiscoveryContext, homepage: string | null): string | null {
  const url = homepage ? normalizeUrl(homepage) : null
  const host = url ? normalizedDomain(url) : null
  if (!url || !host || isGithubWebsite(url) || hasPath(url)) {
    return null
  }
  if (PACKAGE_REGISTRIES.some((registry) => host === registry || host.endsWith(`.${registry}`))) {
    return null
  }
  const websiteBase = ctx.websiteShared && ctx.website ? docsBaseDomain(ctx.website) : null
  return websiteBase !== null && websiteBase === docsBaseDomain(url) ? null : url
}

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
    const docsDomain = docsBaseDomain(website)
    if (!normalized || !domain || !docsDomain) {
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
        ...(pathScopedOnly ? [] : [`https://docs.${docsDomain}`, rootBase]),
      ]),
    ]

    for (const base of bases) {
      if (isLlmsTxtBody(await fetchText(`${base}/llms.txt`, 5_000, true))) {
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
    const domain = docsBaseDomain(website)
    if (!domain) {
      return []
    }
    const url = `https://docs.${domain}`
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

    const links = new Map<string, { url: string; text: string }>()
    const addLink = (raw: string, text: string) => {
      const key = normalizeUrl(raw)
      if (!key) {
        return
      }
      const known = links.get(key)
      if (!known || (!DOCS_KEYWORDS.test(known.text) && DOCS_KEYWORDS.test(text))) {
        links.set(key, { url: raw.replace(/#.*$/, ''), text })
      }
    }
    for (const match of readme.matchAll(/\[([^\]]*)\]\((https?:\/\/[^)\s]+)\)/g)) {
      addLink(match[2], match[1])
    }
    for (const match of readme.matchAll(/href="(https?:\/\/[^"]+)"/g)) {
      addLink(match[1], '')
    }

    const homepage = await getRepoHomepage(parsed.owner, parsed.repo, ctx.githubToken)
    const ownBases = [usableWebsite(ctx), usableHomepage(ctx, homepage)].flatMap((site) => {
      const base = site ? docsBaseDomain(site) : null
      return base ? [base] : []
    })
    const ownerPages = `${parsed.owner.toLowerCase()}.github.io`
    const rank = (url: string): number => {
      const host = normalizedDomain(url)
      if (host !== null && ownBases.some((base) => host === base || host.endsWith(`.${base}`))) {
        return 0
      }
      return registrableDomain(url) === ownerPages ? 1 : 2
    }

    const tokens = projectTokens({ name: ctx.name, slug: ctx.slug, repoUrl: repo })
    const filtered: { url: string; fallback?: string }[] = []
    for (const { url, text } of links.values()) {
      if (!DOCS_KEYWORDS.test(text) && !DOCS_KEYWORDS.test(url)) {
        continue
      }
      const foreign = rank(url) === 2
      // Another product's docs (docs.docker.com, docs.conda.io) are linked from many READMEs.
      if (foreign && !isRelevantSerpResult(url, tokens, { hyphenParts: true })) {
        continue
      }
      if (foreign && !DOCS_KEYWORDS.test(text) && !DOCS_KEYWORDS.test(domainOf(url) ?? '')) {
        continue
      }
      if (isReadmeExcludedHost(domainOf(url))) {
        continue
      }
      if (pathSegments(url).some((segment) => EXCLUDED_PATH_SEGMENT.test(segment))) {
        continue
      }
      const collapsed = rank(url) === 0 ? collapseOwnDomainLink(url) : url
      filtered.push({ url: collapsed, fallback: collapsed === url ? undefined : url })
    }

    const byKey = new Map<string, { url: string; fallback?: string }>()
    for (const link of filtered) {
      const key = (normalizeUrl(link.url) ?? link.url).replace(/^https?:\/\/(www\.)?/, '')
      const known = byKey.get(key)
      if (!known) {
        byKey.set(key, link)
      } else if (!known.fallback) {
        known.fallback = link.fallback
      }
    }
    const unique = [...byKey.values()]
    const own = unique
      .filter((link) => rank(link.url) < 2)
      .sort((a, b) => rank(a.url) - rank(b.url))
    const foreign = unique.filter((link) => rank(link.url) === 2)

    const candidates: IDocCandidate[] = []
    const MAX_PROBES = 5
    let probes = 0
    const scan = async (links: { url: string; fallback?: string }[]) => {
      for (const { url, fallback } of links) {
        if (probes >= MAX_PROBES) {
          return
        }
        probes += 1
        if (await isLiveDocs(url)) {
          candidates.push(candidate(url, 'readme-scrape', true))
        } else if (fallback && probes < MAX_PROBES) {
          probes += 1
          if (await isLiveDocs(fallback)) {
            candidates.push(candidate(fallback, 'readme-scrape', true))
          }
        }
      }
    }
    await scan(own)
    if (candidates.length === 0) {
      probes = 0
      await scan(foreign)
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
    const tokens = projectTokens({
      name: ctx.name,
      slug: ctx.slug,
      repoUrl: primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name }),
    })

    const kept = results.filter((result) => {
      if (!result.link || !isRelevantSerpResult(result.link, tokens)) {
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
