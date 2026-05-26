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

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  parseTagValue: false,
  parseAttributeValue: false,
})

export async function resolveLatestVersion(
  groupId: string,
  artifactId: string,
): Promise<string | null> {
  const groupPath = groupId.replace(/\./g, '/')
  const url = `${MAVEN_REPO}/${groupPath}/${artifactId}/maven-metadata.xml`

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
      // Not found is expected for packages that don't exist on Maven Central
      if (err.response?.status === 404) return null
    }
    // Rethrow unexpected errors so callers can decide whether to retry
    throw err
  }
}
