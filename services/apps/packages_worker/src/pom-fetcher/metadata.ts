/**
 * Resolves the latest release version of a Maven artifact using the
 * maven-metadata.xml endpoint on Maven Central.
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

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

export async function resolveLatestVersion(
  groupId: string,
  artifactId: string,
): Promise<string | null> {
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

      return release || latest || null
    } catch (err) {
      if (axios.isAxiosError(err)) {
        if (err.response?.status === 404) return null
        if (err.response?.status === 429 && attempt < MAX_RETRIES) {
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
