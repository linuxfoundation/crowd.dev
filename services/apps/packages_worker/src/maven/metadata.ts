/**
 * Fetches maven-metadata.xml for a Maven artifact and returns the full version
 * list plus the current release version.
 *
 * URL format:
 *   https://repo1.maven.org/maven2/{groupPath}/{artifactId}/maven-metadata.xml
 *
 * Returns null when the artifact is not found (404) or the metadata is
 * malformed.
 */
import axios from 'axios'
import { XMLParser } from 'fast-xml-parser'

import { isPrerelease } from './normalize'
import { JITPACK_BASE_URL, resolveRegistryBaseUrl } from './registry'

const REQUEST_TIMEOUT_MS = 10_000
const MAX_RETRIES = 3
const RETRY_BASE_MS = 2_000

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  parseTagValue: false,
  parseAttributeValue: false,
})

export interface MavenVersionsMetadata {
  versions: string[]
  releaseVersion: string | null
  lastUpdated: Date | null
}

// maven-metadata.xml carries <lastUpdated>yyyyMMddHHmmss</lastUpdated> in UTC —
// the timestamp of the most recent publish. Parse it into a Date for
// packages.latest_release_at; return null on anything malformed.
function parseMavenLastUpdated(raw: unknown): Date | null {
  if (typeof raw !== 'string') {
    // fast-xml-parser may coerce the all-digits value to a number
    raw = typeof raw === 'number' ? String(raw) : null
  }
  const m =
    typeof raw === 'string'
      ? raw.trim().match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/)
      : null
  if (!m) return null
  const [, y, mo, d, h, mi, s] = m
  const ts = Date.UTC(+y, +mo - 1, +d, +h, +mi, +s)
  return Number.isNaN(ts) ? null : new Date(ts)
}

export type MavenFetchError =
  | { kind: 'NOT_FOUND' }
  | { kind: 'RATE_LIMIT'; status: number }
  | { kind: 'TRANSIENT'; message: string }

export function isMavenFetchError(v: unknown): v is MavenFetchError {
  return typeof v === 'object' && v !== null && 'kind' in v
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

/**
 * Returns the best stable release version given Maven's candidate (from <release>/<latest>)
 * and the full versions list. Falls back to the pre-release candidate only when no stable
 * version exists at all (e.g. a library that has never cut a stable release).
 */
export function pickStableRelease(candidate: string | null, versions: string[]): string | null {
  if (candidate && !isPrerelease(candidate)) return candidate
  const latestStable = [...versions].reverse().find((v) => !isPrerelease(v)) ?? null
  return latestStable ?? candidate
}

export async function resolveVersionsList(
  groupId: string,
  artifactId: string,
  baseUrl?: string,
): Promise<MavenVersionsMetadata | MavenFetchError> {
  const groupPath = groupId.replace(/\./g, '/')
  const url = `${baseUrl ?? resolveRegistryBaseUrl(groupId)}/${groupPath}/${artifactId}/maven-metadata.xml`

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await axios.get<string>(url, {
        responseType: 'text',
        timeout: REQUEST_TIMEOUT_MS,
      })
      const parsed = parser.parse(res.data)

      const versioning = parsed?.metadata?.versioning
      const release = typeof versioning?.release === 'string' ? versioning.release.trim() : null
      const latest = typeof versioning?.latest === 'string' ? versioning.latest.trim() : null

      const rawVersions = versioning?.versions?.version
      let versions: string[] = []
      if (Array.isArray(rawVersions)) {
        versions = rawVersions.map((v: unknown) => String(v).trim()).filter(Boolean)
      } else if (typeof rawVersions === 'string' && rawVersions.trim()) {
        versions = [rawVersions.trim()]
      }

      const releaseVersion = pickStableRelease(release || latest || null, versions)

      return {
        versions,
        releaseVersion,
        lastUpdated: parseMavenLastUpdated(versioning?.lastUpdated),
      }
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const status = err.response?.status
        // 404 = standard not-found. 401 = JitPack-specific: returned for packages
        // that don't exist as public builds. Scoped to JitPack only — other
        // registries may return 401 for auth failures, which should not be
        // silently treated as a missing package.
        if (status === 404 || (status === 401 && url.startsWith(JITPACK_BASE_URL + '/')))
          return { kind: 'NOT_FOUND' }
        // 429 = explicit rate limit, 403 = CDN throttle (Maven Central uses both)
        if ((status === 429 || status === 403) && attempt < MAX_RETRIES) {
          const delay = RETRY_BASE_MS * 2 ** attempt + Math.random() * 500
          await sleep(delay)
          continue
        }
        if (status === 429 || status === 403) return { kind: 'RATE_LIMIT', status }
      }
      const message = err instanceof Error ? err.message : String(err)
      return { kind: 'TRANSIENT', message }
    }
  }

  return { kind: 'RATE_LIMIT', status: 429 }
}

export async function resolveLatestVersion(
  groupId: string,
  artifactId: string,
  baseUrl?: string,
): Promise<string | null> {
  const meta = await resolveVersionsList(groupId, artifactId, baseUrl)
  if (isMavenFetchError(meta)) return null
  return meta.releaseVersion
}
