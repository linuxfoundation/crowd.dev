/**
 * Core POM extraction logic — HTTP only, no DB calls.
 * Callers are responsible for concurrency and persistence.
 */
import axios from 'axios'
import { XMLParser } from 'fast-xml-parser'

import { getServiceChildLogger } from '@crowd/logging'

import { resolveRegistryBaseUrl } from './registry'

const log = getServiceChildLogger('maven')

// ─── Types ────────────────────────────────────────────────────────────────────

export interface PomMaintainer {
  username: string | null
  displayName: string | null
  email: string | null
  url: string | null
  role: 'author' | 'maintainer'
}

export interface PomExtractionResult {
  groupId: string
  artifactId: string
  version: string
  purl: string
  description: string | null
  licenses: string[]
  licensesRaw: string | null
  scmUrl: string | null
  homepageUrl: string | null
  developers: PomMaintainer[]
  contributors: PomMaintainer[]
  parentHops: number
  error: string | null
}

// ─── Internal POM types ───────────────────────────────────────────────────────

interface PomData {
  description?: unknown
  url?: unknown
  licenses?: { license?: unknown }
  scm?: { url?: unknown; connection?: unknown }
  developers?: { developer?: unknown }
  contributors?: { contributor?: unknown }
  parent?: { groupId?: unknown; artifactId?: unknown; version?: unknown }
  properties?: unknown
}

interface PomPerson {
  id?: unknown
  name?: unknown
  email?: unknown
  url?: unknown
}

// ─── Config ───────────────────────────────────────────────────────────────────

const REQUEST_TIMEOUT_MS = 15_000

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  parseTagValue: false, // keep all values as strings — prevents version "65" becoming number
  parseAttributeValue: false,
})

// ─── Retry with exponential backoff ──────────────────────────────────────────

const MAX_RETRIES = 3
const RETRY_BASE_MS = 2_000

// prettier-ignore
async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// prettier-ignore
async function getWithRetry(url: string): Promise<string> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await axios.get<string>(url, {
        responseType: 'text',
        timeout: REQUEST_TIMEOUT_MS,
      })
      return res.data
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const status = err.response?.status
        // 429 = explicit rate limit, 403 = CDN throttle (Maven Central uses both)
        if ((status === 429 || status === 403) && attempt < MAX_RETRIES) {
          const delay = RETRY_BASE_MS * 2 ** attempt + Math.random() * 500
          await sleep(delay)
          continue
        }
      }
      throw err
    }
  }
  throw new Error(`Max retries exceeded for ${url}`)
}

// ─── POM fetch ────────────────────────────────────────────────────────────────

export function buildPomUrl(
  groupId: string,
  artifactId: string,
  version: string,
  baseUrl?: string,
): string {
  const groupPath = groupId.replace(/\./g, '/')
  return `${baseUrl ?? resolveRegistryBaseUrl(groupId)}/${groupPath}/${artifactId}/${version}/${artifactId}-${version}.pom`
}

// prettier-ignore
export async function fetchPom(groupId: string, artifactId: string, version: string, url: string): Promise<PomData | null> {
  try {
    const data = await getWithRetry(url)
    const parsed = parser.parse(data)
    return (parsed?.project as PomData) ?? null
  } catch (err) {
    if (axios.isAxiosError(err)) {
      const status = err.response?.status
      if (status === 404) {
        log.debug({ groupId, artifactId, version }, `POM not found (404): ${url}`)
        return null
      }
      log.debug(
        { groupId, artifactId, version },
        `HTTP ${status ?? 'unknown'} fetching POM: ${url}`,
      )
      return null
    }
    throw err
  }
}

// ─── POM cache ──────────────────────────────────────────────────────────────
//
// Parent POMs are heavily shared across artifacts of the same namespace
// (e.g. org.apache:apache, org.springframework.boot:spring-boot-starter-parent),
// and the critical batch is re-sorted by namespace before processing (see
// runMavenEnrichmentLoop), so those siblings are processed close together. A
// module-level, coordinate-keyed in-process cache
// collapses those repeated parent fetches into a single HTTP request — the single
// biggest lever against Maven Central rate limiting. It also removes the redundant
// second fetch of each artifact's own POM (extractArtifact fetches the leaf, then
// resolveWithInheritance fetches it again at depth 0).
//
// Only *successful* fetches are cached: fetchPom() returns null for both a real 404
// and a transient failure (throttle/timeout), so caching null would poison the cache
// with transient errors — we never do it. Maven coordinates are immutable, so a cached
// POM never goes stale; the LRU size cap is purely to bound memory.

const POM_CACHE_MAX_ENTRIES = 5_000

const pomCache = new Map<string, PomData>()
// prettier-ignore
const inFlight = new Map<string, Promise<PomData | null>>()
const pomCacheStats = { hits: 0, coalesced: 0, misses: 0, evictions: 0 }

function pomCacheKey(groupId: string, artifactId: string, version: string): string {
  return `${groupId}:${artifactId}:${version}`
}

function cacheSet(key: string, pom: PomData): void {
  pomCache.delete(key) // re-insert to refresh recency (LRU)
  pomCache.set(key, pom)
  if (pomCache.size > POM_CACHE_MAX_ENTRIES) {
    const oldest = pomCache.keys().next().value
    if (oldest !== undefined) {
      pomCache.delete(oldest)
      pomCacheStats.evictions++
    }
  }
}

/**
 * Cached + request-coalescing wrapper around fetchPom().
 * - Cache hit  → returns the stored POM, no HTTP.
 * - In-flight  → a concurrent fetch for the same coordinates is already running;
 *                await it instead of issuing a duplicate request.
 * - Miss       → performs the network fetch; caches the result only if non-null.
 */
// prettier-ignore
async function fetchPomCached(groupId: string, artifactId: string, version: string, baseUrl?: string): Promise<PomData | null> {
  const key = pomCacheKey(groupId, artifactId, version)

  const cached = pomCache.get(key)
  if (cached !== undefined) {
    pomCacheStats.hits++
    pomCache.delete(key) // refresh recency on read (LRU)
    pomCache.set(key, cached)
    return cached
  }

  const pending = inFlight.get(key)
  if (pending) {
    pomCacheStats.coalesced++
    return pending
  }

  pomCacheStats.misses++
  const promise = fetchPom(
    groupId,
    artifactId,
    version,
    buildPomUrl(groupId, artifactId, version, baseUrl),
  )
    .then((pom) => {
      if (pom) cacheSet(key, pom)
      return pom
    })
    .finally(() => {
      inFlight.delete(key)
    })

  inFlight.set(key, promise)
  return promise
}

/** Snapshot of cache effectiveness — logged once per critical batch by the enrichment loop. */
export function getPomCacheStats(): {
  size: number
  hits: number
  coalesced: number
  misses: number
  evictions: number
  hitRate: number
} {
  const lookups = pomCacheStats.hits + pomCacheStats.coalesced + pomCacheStats.misses
  const hitRate =
    lookups === 0
      ? 0
      : Math.round(((pomCacheStats.hits + pomCacheStats.coalesced) / lookups) * 100) / 100
  return { size: pomCache.size, ...pomCacheStats, hitRate }
}

/** Clears the cache and counters. Intended for tests. */
export function resetPomCache(): void {
  pomCache.clear()
  inFlight.clear()
  pomCacheStats.hits = 0
  pomCacheStats.coalesced = 0
  pomCacheStats.misses = 0
  pomCacheStats.evictions = 0
}

// ─── Inheritance resolution ───────────────────────────────────────────────────

// Covers all known real-world Maven parent chains (Spring, Apache, etc. top out at ~4 hops).
const MAX_PARENT_DEPTH = 8

interface ResolvedFields {
  description: string | null
  licenses: string[]
  licensesRaw: string | null
  scmUrl: string | null
  homepageUrl: string | null
  developers: PomMaintainer[]
  contributors: PomMaintainer[]
  hops: number
  // Merged <properties> across the resolved parent chain (child overrides parent),
  // used to interpolate ${...} placeholders in the SCM URL.
  properties: Record<string, string>
}

// prettier-ignore
async function resolveWithInheritance(groupId: string, artifactId: string, version: string, depth = 0, visited = new Set<string>(), baseUrl?: string): Promise<ResolvedFields> {
  const pom = await fetchPomCached(groupId, artifactId, version, baseUrl)
  if (!pom) return emptyFields(depth)

  const licenses = extractLicenses(pom)
  const scmUrl = extractStr(pom.scm?.url ?? pom.scm?.connection)
  const developers = extractPersons(pom.developers?.developer, 'author')
  const contributors = extractPersons(pom.contributors?.contributor, 'maintainer')
  const properties = extractProperties(pom)

  const missingLicense = licenses.length === 0
  // An unresolved ${...} placeholder counts as missing: the property that defines it
  // may live in a parent POM, so we still need to walk the chain to collect it.
  const missingScm = !scmUrl || scmUrl.includes('${')
  const missingDevelopers = developers.length === 0 || contributors.length === 0
  const parent = extractParent(pom)

  if (parent && (missingLicense || missingScm || missingDevelopers)) {
    const parentKey = `${parent.groupId}:${parent.artifactId}:${parent.version}`
    if (depth >= MAX_PARENT_DEPTH || visited.has(parentKey)) {
      log.warn(
        { groupId, artifactId, version, depth, cycle: visited.has(parentKey) },
        'Parent chain limit reached — stopping inheritance resolution',
      )
    } else {
      visited.add(parentKey)
      log.debug({ groupId, artifactId, version }, `[hop ${depth + 1}] ${parentKey}`)
      const parentFields = await resolveWithInheritance(
        parent.groupId,
        parent.artifactId,
        parent.version,
        depth + 1,
        visited,
        resolveRegistryBaseUrl(parent.groupId),
      )
      return {
        description: extractStr(pom.description) ?? parentFields.description,
        licenses: licenses.length > 0 ? licenses : parentFields.licenses,
        licensesRaw: licenses.length > 0 ? licenses.join(', ') : parentFields.licensesRaw,
        scmUrl: scmUrl ?? parentFields.scmUrl,
        homepageUrl: extractStr(pom.url) ?? parentFields.homepageUrl,
        developers: developers.length > 0 ? developers : parentFields.developers,
        contributors: contributors.length > 0 ? contributors : parentFields.contributors,
        hops: parentFields.hops,
        // Child properties override the parent's.
        properties: { ...parentFields.properties, ...properties },
      }
    }
  }

  return {
    description: extractStr(pom.description),
    licenses,
    licensesRaw: licenses.length > 0 ? licenses.join(', ') : null,
    scmUrl,
    homepageUrl: extractStr(pom.url),
    developers,
    contributors,
    hops: depth,
    properties,
  }
}

// ─── Public entry points ──────────────────────────────────────────────────────

/**
 * Fetches only the root POM without following the parent chain — faster than
 * extractArtifact, but inherited fields (licenses, SCM) may be missing.
 * Currently unused: kept as a lightweight option for high-throughput paths that
 * don't need parent inheritance.
 */
// prettier-ignore
export async function extractArtifactDirect(groupId: string, artifactId: string, version: string, baseUrl?: string): Promise<PomExtractionResult> {
  const purl = `pkg:maven/${groupId}/${artifactId}@${version}`
  const pomUrl = buildPomUrl(groupId, artifactId, version, baseUrl)
  const pom = await fetchPomCached(groupId, artifactId, version, baseUrl)

  if (!pom) {
    return {
      groupId,
      artifactId,
      version,
      purl,
      description: null,
      licenses: [],
      licensesRaw: null,
      scmUrl: null,
      homepageUrl: null,
      developers: [],
      contributors: [],
      parentHops: 0,
      error: `POM not found: ${pomUrl}`,
    }
  }

  const licenses = extractLicenses(pom)
  const rawScmUrl = extractStr(pom.scm?.url ?? pom.scm?.connection)
  const props = {
    ...extractProperties(pom),
    ...builtinProjectProperties(groupId, artifactId, version),
  }
  const scmUrl = rawScmUrl ? interpolateProperties(rawScmUrl, props) : null
  const developers = extractPersons(pom.developers?.developer, 'author')
  const contributors = extractPersons(pom.contributors?.contributor, 'maintainer')

  return {
    groupId,
    artifactId,
    version,
    purl,
    description: extractStr(pom.description),
    licenses,
    licensesRaw: licenses.length > 0 ? licenses.join(', ') : null,
    scmUrl,
    homepageUrl: extractStr(pom.url),
    developers,
    contributors,
    parentHops: 0,
    error: null,
  }
}

/**
 * Fetches and resolves POM metadata for the given Maven artifact, following
 * the parent chain to inherit licenses and SCM when not in the direct POM.
 * Always returns a result object; errors are captured in `result.error`.
 */
// prettier-ignore
export async function extractArtifact(groupId: string, artifactId: string, version: string, baseUrl?: string): Promise<PomExtractionResult> {
  const purl = `pkg:maven/${groupId}/${artifactId}@${version}`

  const pomUrl = buildPomUrl(groupId, artifactId, version, baseUrl)
  const rootPom = await fetchPomCached(groupId, artifactId, version, baseUrl)
  if (!rootPom) {
    return {
      groupId,
      artifactId,
      version,
      purl,
      description: null,
      licenses: [],
      licensesRaw: null,
      scmUrl: null,
      homepageUrl: null,
      developers: [],
      contributors: [],
      parentHops: 0,
      error: `POM not found: ${pomUrl}`,
    }
  }

  try {
    const resolved = await resolveWithInheritance(
      groupId,
      artifactId,
      version,
      0,
      new Set(),
      baseUrl,
    )
    // Resolve ${...} placeholders in the SCM URL using the merged chain properties
    // plus the leaf's built-in project.* values. Best-effort: unresolved placeholders
    // stay literal and are rejected downstream by normalizeScmUrl.
    const props = {
      ...resolved.properties,
      ...builtinProjectProperties(groupId, artifactId, version),
    }
    const scmUrl = resolved.scmUrl ? interpolateProperties(resolved.scmUrl, props) : null
    return {
      groupId,
      artifactId,
      version,
      purl,
      description: resolved.description,
      licenses: resolved.licenses,
      licensesRaw: resolved.licensesRaw,
      scmUrl,
      homepageUrl: resolved.homepageUrl,
      developers: resolved.developers,
      contributors: resolved.contributors,
      parentHops: resolved.hops,
      error: null,
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    log.debug({ groupId, artifactId, version }, `Error resolving POM: ${message}`)
    return {
      groupId,
      artifactId,
      version,
      purl,
      description: null,
      licenses: [],
      licensesRaw: null,
      scmUrl: null,
      homepageUrl: null,
      developers: [],
      contributors: [],
      parentHops: 0,
      error: message,
    }
  }
}

// ─── SCM URL normalisation ───────────────────────────────────────────────────

/**
 * Known source-code-hosting hosts. A normalised repository_url is only produced
 * when the URL resolves to one of these — anything else (homepages, doc sites,
 * placeholders) yields null so it is never stored as a repository link.
 *
 * TODO(CM): host list pending product confirmation before rollout.
 *
 * Candidate additions found in Maven declared_repository_url (critical rows) but
 * intentionally NOT added yet, because they need more than a host allowlist:
 *   - gitbox.apache.org / git.apache.org / git-wip-us.apache.org (~1.3k rows):
 *     paths are `/repos/asf/<repo>`, so the generic first-two-segments logic would
 *     collapse every Apache repo to `repos/asf`. Needs path-aware handling (skip
 *     the `/repos/asf/` prefix) or mapping to the github.com/apache mirror.
 *   - git.eclipse.org (~180 rows): Gerrit paths, likewise not owner/repo.
 *   - android.googlesource.com, ec.europa.eu (Bitbucket-Server /projects/x/repos/y):
 *     multi-segment paths, not owner/repo.
 *   - ambiguous `git.*` hosts (git.iem.at, git.i-novus.ru, git.oschina.net) and the
 *     internal git.corp.adobe.com: pending path/reachability confirmation.
 */
const SCM_HOSTS = new Set([
  'github.com',
  'gitlab.com',
  'bitbucket.org',
  'gitee.com',
  'codeberg.org',
  // Self-hosted GitLab / Gitea instances seen in Maven POMs with clean
  // /<owner>/<repo> paths (same shape as gitlab.com — handled by the generic logic).
  // Internal-only hosts (git.corp.adobe.com, gitlab.alibaba-inc.com) are excluded:
  // their links are unreachable for consumers. The ≥2-segment owner/repo requirement
  // acts as a safety net so a mis-classified host yields NULL, never a junk link.
  'gitlab.smartb.city',
  'gitlab.ow2.org',
  'gitlab.nuiton.org',
  'gitlab.inria.fr',
  'git.neckar.it',
  'git.iem.at',
  'git.oschina.net',
  'git.i-novus.ru',
  'gitlab.protontech.ch',
  'git.catchpoint.net',
  'git.dorkbox.com',
  'git.adorsys.de',
])

/** Hosts whose owner/repo path is case-insensitive and should be lower-cased. */
const CASE_INSENSITIVE_HOSTS = new Set(['github.com', 'gitlab.com'])

/**
 * Converts the raw SCM URL from a POM (declared_repository_url) into a clean,
 * canonical `https://<host>/<owner>/<repo>` repository URL suitable for storage
 * as repository_url. Returns null when the input does not resolve to a real
 * repository on a known SCM host.
 *
 * Handles common Maven SCM URL forms:
 *   scm:git:git@github.com:owner/repo.git    → https://github.com/owner/repo
 *   scm:git:https://github.com/owner/repo    → https://github.com/owner/repo
 *   scm:git:github.com/owner/repo            → https://github.com/owner/repo
 *   github.com/owner/repo (no scheme)        → https://github.com/owner/repo
 *   git://github.com/owner/repo.git          → https://github.com/owner/repo
 *   http://github.com/owner/repo/tree/...    → https://github.com/owner/repo
 *   github.com:owner/repo.git (SCP colon)    → https://github.com/owner/repo
 *   https://github.com:owner/repo            → https://github.com/owner/repo
 *   ssh://git@github.com:owner/repo.git      → https://github.com/owner/repo
 *
 * Rejected (→ null): website-only URLs (https://meson.ai/), non-SCM hosts
 * (svn://…, http://source.android.com), placeholders (Private, ${scm-url}).
 */
export function normalizeScmUrl(raw: string | null): string | null {
  if (!raw) return null
  let s = raw.trim()
  if (!s) return null

  // A leftover ${...} means best-effort interpolation upstream could not resolve it.
  // Reject outright: a placeholder embedded in the path (e.g. github.com/owner/${x})
  // otherwise survives URL parsing (percent-encoded to %7B…%7D) and would yield a junk
  // repository_url instead of null.
  if (s.includes('${')) return null

  // Strip Maven scm:git: / scm: prefix
  s = s.replace(/^scm:git:/i, '').replace(/^scm:/i, '')

  // git+https://… → https://…
  s = s.replace(/^git\+/, '')

  // SCP form git@host:owner/repo → https://host/owner/repo
  s = s.replace(/^git@([^:/]+):(.+)$/, 'https://$1/$2')

  // ssh://git@host:owner/repo → https://host/owner/repo (SCP colon under ssh)
  s = s.replace(/^ssh:\/\/git@([^:/]+):(?=\D)/, 'https://$1/')

  // ssh://git@host/… → https://host/…
  s = s.replace(/^ssh:\/\/git@([^/]+)\//, 'https://$1/')

  // git:// → https://, and upgrade http:// → https:// — done before the SCP-colon
  // rule below so that git://host:owner/repo is normalised too.
  s = s.replace(/^git:\/\//, 'https://').replace(/^http:\/\//, 'https://')

  // scheme://host:owner/repo → scheme://host/owner/repo — the colon is an SCP path
  // separator, not a port (guarded by \D so real numeric ports are left intact).
  s = s.replace(/^(https?):\/\/([^:/]+):(?=\D)/, '$1://$2/')

  // No scheme at all (e.g. "github.com/owner/repo") → assume https
  if (!s.includes('://')) {
    // Bare SCP form "host:owner/repo" → "host/owner/repo" before assuming https.
    s = s.replace(/^([^/:]+):(?=\D)/, '$1/')
    s = `https://${s}`
  }

  let parsed: URL
  try {
    parsed = new URL(s)
  } catch {
    return null
  }

  if (parsed.protocol !== 'https:') return null

  const host = parsed.hostname.toLowerCase().replace(/^www\./, '')
  if (!SCM_HOSTS.has(host)) return null

  // Require at least owner + repo path segments
  const segments = parsed.pathname.split('/').filter(Boolean)
  if (segments.length < 2) return null

  let owner = segments[0]
  let name = segments[1].replace(/\.git$/, '')
  if (!owner || !name) return null

  if (CASE_INSENSITIVE_HOSTS.has(host)) {
    owner = owner.toLowerCase()
    name = name.toLowerCase()
  }

  return `https://${host}/${owner}/${name}`
}

// ─── Private helpers ──────────────────────────────────────────────────────────

function extractStr(value: unknown): string | null {
  if (typeof value === 'string' && value.trim()) return value.trim()
  return null
}

function extractLicenses(pom: PomData): string[] {
  const raw = pom.licenses?.license
  if (!raw) return []
  const list = Array.isArray(raw) ? raw : [raw]
  return (list as { name?: unknown }[])
    .map((l) => extractStr(l?.name))
    .filter((n): n is string => n !== null)
}

function extractPersons(raw: unknown, role: 'author' | 'maintainer'): PomMaintainer[] {
  if (!raw) return []
  const list = Array.isArray(raw) ? raw : [raw]
  return (list as PomPerson[])
    .filter((p) => p.id || p.name || p.email)
    .map((p) => ({
      username: extractStr(p.id),
      displayName: extractStr(p.name),
      email: extractStr(p.email),
      url: extractStr(p.url),
      role,
    }))
}

/** Flattens a POM's <properties> block into a string→string map (non-string values skipped). */
function extractProperties(pom: PomData): Record<string, string> {
  const raw = pom.properties
  if (!raw || typeof raw !== 'object') return {}
  const out: Record<string, string> = {}
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof value === 'string') out[key] = value
    else if (typeof value === 'number') out[key] = String(value)
  }
  return out
}

/** Maven built-in project.* / pom.* properties for the leaf coordinates. */
function builtinProjectProperties(
  groupId: string,
  artifactId: string,
  version: string,
): Record<string, string> {
  return {
    'project.groupId': groupId,
    'project.artifactId': artifactId,
    'project.version': version,
    'pom.groupId': groupId,
    'pom.artifactId': artifactId,
    'pom.version': version,
    groupId,
    artifactId,
    version,
  }
}

const MAX_INTERPOLATION_DEPTH = 10

/**
 * Best-effort Maven property interpolation of ${...} placeholders. Resolves
 * recursively (a property value may itself reference another) up to a depth cap
 * to guard against cycles. Placeholders with no matching property (e.g. defined
 * in a profile/settings, or method calls like ${x.substring(8)}) are left as-is
 * so the SCM normaliser rejects them.
 */
export function interpolateProperties(value: string, props: Record<string, string>): string {
  let current = value
  for (let i = 0; i < MAX_INTERPOLATION_DEPTH && current.includes('${'); i++) {
    const next = current.replace(/\$\{([^{}]+)\}/g, (match, key) => {
      const resolved = props[(key as string).trim()]
      return resolved !== undefined ? resolved : match
    })
    if (next === current) break
    current = next
  }
  return current
}

function extractParent(
  pom: PomData,
): { groupId: string; artifactId: string; version: string } | null {
  const p = pom.parent
  if (!p) return null
  const groupId = extractStr(p.groupId)
  const artifactId = extractStr(p.artifactId)
  const version = extractStr(p.version)
  if (!groupId || !artifactId || !version) return null
  return { groupId, artifactId, version }
}

function emptyFields(hops: number): ResolvedFields {
  return {
    description: null,
    licenses: [],
    licensesRaw: null,
    scmUrl: null,
    homepageUrl: null,
    developers: [],
    contributors: [],
    hops,
    properties: {},
  }
}
