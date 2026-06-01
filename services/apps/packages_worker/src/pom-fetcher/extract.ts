/**
 * Core POM extraction logic — pure functions (no I/O side-effects, no DB calls).
 * Callers are responsible for concurrency, retries, and persistence.
 */

import axios from 'axios'
import { XMLParser } from 'fast-xml-parser'

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
}

interface PomPerson {
  id?: unknown
  name?: unknown
  email?: unknown
  url?: unknown
}

// ─── Config ───────────────────────────────────────────────────────────────────

const MAVEN_REPO = 'https://repo1.maven.org/maven2'
export const MAX_PARENT_HOPS = 7
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

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

async function getWithRetry(url: string): Promise<string> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await axios.get<string>(url, { responseType: 'text', timeout: REQUEST_TIMEOUT_MS })
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

export function buildPomUrl(groupId: string, artifactId: string, version: string): string {
  const groupPath = groupId.replace(/\./g, '/')
  return `${MAVEN_REPO}/${groupPath}/${artifactId}/${version}/${artifactId}-${version}.pom`
}

export async function fetchPom(
  groupId: string,
  artifactId: string,
  version: string,
  log?: (msg: string) => void,
): Promise<PomData | null> {
  const url = buildPomUrl(groupId, artifactId, version)
  try {
    const data = await getWithRetry(url)
    const parsed = parser.parse(data)
    return (parsed?.project as PomData) ?? null
  } catch (err) {
    if (axios.isAxiosError(err)) {
      const status = err.response?.status
      if (status === 404) {
        log?.(`POM not found (404): ${url}`)
        return null
      }
      log?.(`HTTP ${status ?? 'unknown'} fetching POM: ${url}`)
      return null
    }
    throw err
  }
}

// ─── Inheritance resolution ───────────────────────────────────────────────────

interface ResolvedFields {
  description: string | null
  licenses: string[]
  licensesRaw: string | null
  scmUrl: string | null
  homepageUrl: string | null
  developers: PomMaintainer[]
  contributors: PomMaintainer[]
  hops: number
}

async function resolveWithInheritance(
  groupId: string,
  artifactId: string,
  version: string,
  log: (msg: string) => void,
  depth = 0,
): Promise<ResolvedFields> {
  if (depth > MAX_PARENT_HOPS) {
    log(`Max parent hops (${MAX_PARENT_HOPS}) reached`)
    return emptyFields(depth)
  }

  const pom = await fetchPom(groupId, artifactId, version, log)
  if (!pom) return emptyFields(depth)

  const licenses = extractLicenses(pom)
  const scmUrl = extractStr(pom.scm?.url ?? pom.scm?.connection)
  const developers = extractPersons(pom.developers?.developer, 'author')
  const contributors = extractPersons(pom.contributors?.contributor, 'maintainer')

  const missingLicense = licenses.length === 0
  const missingScm = !scmUrl
  const parent = extractParent(pom)

  if (parent && (missingLicense || missingScm)) {
    log(`[hop ${depth + 1}] ${parent.groupId}:${parent.artifactId}:${parent.version}`)
    const parentFields = await resolveWithInheritance(
      parent.groupId,
      parent.artifactId,
      parent.version,
      log,
      depth + 1,
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
  }
}

// ─── Public entry points ──────────────────────────────────────────────────────

/**
 * Fetches only the root POM without following the parent chain.
 * Faster than extractArtifact — use for non-critical packages where inherited
 * fields (licenses, SCM) may be missing but throughput matters more.
 */
export async function extractArtifactDirect(
  groupId: string,
  artifactId: string,
  version: string,
  log: (msg: string) => void = () => undefined,
): Promise<PomExtractionResult> {
  const purl = `pkg:maven/${groupId}/${artifactId}@${version}`
  const pom = await fetchPom(groupId, artifactId, version, log)

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
      error: `POM not found: ${buildPomUrl(groupId, artifactId, version)}`,
    }
  }

  const licenses = extractLicenses(pom)
  const scmUrl = extractStr(pom.scm?.url ?? pom.scm?.connection)
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
export async function extractArtifact(
  groupId: string,
  artifactId: string,
  version: string,
  log: (msg: string) => void = () => undefined,
): Promise<PomExtractionResult> {
  const purl = `pkg:maven/${groupId}/${artifactId}@${version}`

  const rootPom = await fetchPom(groupId, artifactId, version, log)
  if (!rootPom) {
    const pomUrl = buildPomUrl(groupId, artifactId, version)
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
    const resolved = await resolveWithInheritance(groupId, artifactId, version, log)
    return {
      groupId,
      artifactId,
      version,
      purl,
      description: resolved.description,
      licenses: resolved.licenses,
      licensesRaw: resolved.licensesRaw,
      scmUrl: resolved.scmUrl,
      homepageUrl: resolved.homepageUrl,
      developers: resolved.developers,
      contributors: resolved.contributors,
      parentHops: resolved.hops,
      error: null,
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    log(`Error resolving POM: ${message}`)
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
 * Converts the raw SCM URL from a POM (declared_repository_url) into a clean
 * HTTPS repository URL suitable for storage as repository_url.
 *
 * Handles common Maven SCM URL forms:
 *   scm:git:git@github.com:owner/repo.git  → https://github.com/owner/repo
 *   scm:git:https://github.com/owner/repo  → https://github.com/owner/repo
 *   git://github.com/owner/repo.git        → https://github.com/owner/repo
 *   https://github.com/owner/repo/tree/... → https://github.com/owner/repo
 */
export function normalizeScmUrl(raw: string | null): string | null {
  if (!raw) return null
  let url = raw.trim()

  // Strip scm:git: or scm: prefix
  url = url.replace(/^scm:git:/i, '').replace(/^scm:/i, '')

  // Convert SSH git@host:owner/repo → https://host/owner/repo
  url = url.replace(/^git@([^:]+):(.+)$/, 'https://$1/$2')

  // Convert git:// → https://
  url = url.replace(/^git:\/\//, 'https://')

  // Strip trailing .git
  url = url.replace(/\.git$/, '')

  // Strip /tree/... or /blob/... path suffixes (keep only host + owner + repo)
  url = url.replace(/\/(tree|blob)(\/.*)?$/, '')

  if (!url.startsWith('https://')) return null

  return url.replace(/\/$/, '')
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
  return (list as Array<{ name?: unknown }>)
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
  }
}
