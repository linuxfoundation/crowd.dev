/**
 * Maps Maven groupId prefixes to their authoritative repository.
 *
 * Maven Central is the default, but several large ecosystems publish exclusively
 * to their own Maven-compatible repository and will always 404 on Central:
 *   - Google Maven  (androidx.*, com.google.android.*, com.google.firebase.*)
 *   - Gradle Plugin Portal  (gradle.plugin.*)
 *   - Jenkins  (org.jenkins-ci.*, io.jenkins.*)
 *
 * All these repos expose the standard Maven repository layout, so the same
 * metadata.xml / POM fetch logic works — only the base URL changes.
 */

const MAVEN_CENTRAL_BASE_URL = 'https://repo1.maven.org/maven2'

interface RegistryEntry {
  prefix: string
  baseUrl: string
  pageUrl: (groupId: string, artifactId: string) => string
}

const ALTERNATIVE_REGISTRIES: RegistryEntry[] = [
  {
    prefix: 'androidx',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.google.android',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.google.firebase',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'gradle.plugin',
    baseUrl: 'https://plugins.gradle.org/m2',
    pageUrl: (g, a) => `https://plugins.gradle.org/m2/${g.replace(/\./g, '/')}/${a}/`,
  },
  {
    prefix: 'org.jenkins-ci',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
  {
    prefix: 'io.jenkins',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
]

function findEntry(groupId: string): RegistryEntry | undefined {
  return ALTERNATIVE_REGISTRIES.find((r) => groupId.startsWith(r.prefix))
}

/**
 * Returns the Maven repository base URL for the given groupId.
 *
 * For known alternative-registry namespaces, always returns the hardcoded
 * URL — the MAVEN_FETCHER_BASE_URL env var (used to point at a GCS mirror for
 * backfill) is intentionally bypassed for these groups because no mirror exists.
 * For everything else, falls back to MAVEN_FETCHER_BASE_URL ?? Maven Central.
 */
export function resolveRegistryBaseUrl(groupId: string): string {
  const entry = findEntry(groupId)
  if (entry) return entry.baseUrl
  return process.env.MAVEN_FETCHER_BASE_URL ?? MAVEN_CENTRAL_BASE_URL
}

/**
 * Returns a human-browsable URL for the given artifact in its authoritative registry.
 */
export function resolveRegistryPageUrl(groupId: string, artifactId: string): string {
  const entry = findEntry(groupId)
  if (entry) return entry.pageUrl(groupId, artifactId)
  return `https://central.sonatype.com/artifact/${groupId}/${artifactId}`
}
