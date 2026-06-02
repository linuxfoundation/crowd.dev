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

const MAVEN_REPO = 'https://repo1.maven.org/maven2'
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
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

export async function resolveVersionsList(
  groupId: string,
  artifactId: string,
): Promise<MavenVersionsMetadata | null> {
  const groupPath = groupId.replace(/\./g, '/')
  const url = `${MAVEN_REPO}/${groupPath}/${artifactId}/maven-metadata.xml`

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await axios.get<string>(url, { responseType: 'text', timeout: REQUEST_TIMEOUT_MS })
      const parsed = parser.parse(res.data)

      // Prefer <release> over <latest> — release excludes snapshots/alphas
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

      return { versions, releaseVersion: release || latest || null }
    } catch (err) {
      if (axios.isAxiosError(err)) {
        if (err.response?.status === 404) return null
        // 429 = explicit rate limit, 403 = CDN throttle (Maven Central uses both)
        if ((err.response?.status === 429 || err.response?.status === 403) && attempt < MAX_RETRIES) {
          const delay = RETRY_BASE_MS * 2 ** attempt + Math.random() * 500
          await sleep(delay)
          continue
        }
      }
      throw err
    }
  }

  return null
}

export async function resolveLatestVersion(
  groupId: string,
  artifactId: string,
): Promise<string | null> {
  const meta = await resolveVersionsList(groupId, artifactId)
  return meta?.releaseVersion ?? null
}
