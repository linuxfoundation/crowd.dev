/**
 * Maps Maven groupId prefixes to their authoritative repository.
 *
 * Maven Central is the default, but several large ecosystems publish exclusively
 * to their own Maven-compatible repository and will always 404 on Central:
 *   - Google Maven  (androidx.*, com.google.android.*, com.google.firebase.*)
 *   - Gradle Plugin Portal  (gradle.plugin.*)
 *   - Jenkins  (org.jenkins-ci.*, io.jenkins.plugins.*, io.jenkins.blueocean.*)
 *
 * All these repos expose the standard Maven repository layout, so the same
 * metadata.xml / POM fetch logic works — only the base URL changes.
 *
 * JitPack (io.github.*, com.github.*) is NOT listed here as a primary registry
 * because many well-known artifacts under those prefixes are published on Maven
 * Central (e.g. io.github.resilience4j, com.github.ben-manes:caffeine). Maven
 * Central is tried first; JitPack is used only as a fallback when Central returns
 * NOT_FOUND (see the enrichment loop).
 */

export const MAVEN_CENTRAL_BASE_URL = 'https://repo1.maven.org/maven2'
export const GRADLE_PLUGIN_PORTAL_BASE_URL = 'https://plugins.gradle.org/m2'
export const JITPACK_BASE_URL = 'https://jitpack.io'

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
  // com.google.android — all artifacts (bare namespace and sub-namespaces like
  // com.google.android.gms.*) are on Google Maven, not Maven Central.
  {
    prefix: 'com.google.android',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  // Pre-AndroidX and Android tooling namespaces — all published on Google Maven.
  // android.arch.* = old Architecture Components (pre-1.0 / pre-AndroidX)
  // com.android.support = Support Library (pre-AndroidX; post-migration moved to androidx.*)
  // com.android.support.test = old Android testing support (pre-AndroidX test, includes Espresso)
  // com.android.tools.build = Android Gradle Plugin, Jetifier, and related build tooling
  // com.android.tools.utp = Android Unified Test Platform
  // com.android.identity = Android Identity Credential library
  {
    prefix: 'android.arch',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.android.identity',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.android.support',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.android.tools.analytics-library',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.android.tools.adblib',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.android.tools.build',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.android.tools.utp',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.google.mediapipe',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.google.testing.platform',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.google.firebase',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'com.google.mlkit',
    baseUrl: 'https://dl.google.com/dl/android/maven2',
    pageUrl: (g, a) => `https://maven.google.com/web/index.html#${g}:${a}`,
  },
  {
    prefix: 'gradle.plugin',
    baseUrl: GRADLE_PLUGIN_PORTAL_BASE_URL,
    pageUrl: (g, a) => `${GRADLE_PLUGIN_PORTAL_BASE_URL}/${g.replace(/\./g, '/')}/${a}/`,
  },
  {
    prefix: 'com.cloudbees.plugins',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
  {
    prefix: 'org.jenkins-ci',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
  // org.jenkinsci.plugins = standard Jenkins community plugin namespace
  {
    prefix: 'org.jenkinsci.plugins',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
  // org.kohsuke.stapler is the Jenkins HTTP routing framework (by Jenkins creator Kohsuke
  // Kawaguchi) — published on Jenkins repo, never on Maven Central.
  {
    prefix: 'org.kohsuke.stapler',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
  // io.jenkins.tools.*, io.jenkins.lib.*, io.jenkins.test.* publish on Maven Central —
  // only the plugin and blueocean sub-namespaces are exclusive to Jenkins repo.
  {
    prefix: 'io.jenkins.plugins',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
  {
    prefix: 'io.jenkins.blueocean',
    baseUrl: 'https://repo.jenkins-ci.org/public',
    pageUrl: (g, a) => `https://repo.jenkins-ci.org/public/${g.replace(/\./g, '/')}/${a}/`,
  },
]

function findEntry(groupId: string): RegistryEntry | undefined {
  return ALTERNATIVE_REGISTRIES.find(
    (r) => groupId === r.prefix || groupId.startsWith(r.prefix + '.'),
  )
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
 * Returns true when the given groupId maps to a non-Central registry.
 * Used in the enrichment loop to decide whether a Central fallback lookup is worth trying
 * (e.g. com.google.firebase:firebase-admin is a server-side Java SDK on Central, while
 * most com.google.firebase artifacts are Android SDKs on Google Maven).
 */
export function isAlternativeRegistry(groupId: string): boolean {
  return findEntry(groupId) !== undefined
}

/**
 * Returns true for io.github.* and com.github.* groupIds — the JitPack namespace
 * convention. Maven Central is tried first for these; JitPack is the fallback.
 */
export function isJitpackNamespace(groupId: string): boolean {
  return (
    groupId === 'io.github' ||
    groupId.startsWith('io.github.') ||
    groupId === 'com.github' ||
    groupId.startsWith('com.github.')
  )
}

/** Returns a human-browsable JitPack URL for an io.github.* or com.github.* artifact. */
export function jitpackPageUrl(groupId: string, artifactId: string): string {
  if (groupId.startsWith('io.github.')) {
    return `${JITPACK_BASE_URL}/#${groupId.slice('io.github.'.length)}/${artifactId}`
  }
  if (groupId.startsWith('com.github.')) {
    return `${JITPACK_BASE_URL}/#${groupId.slice('com.github.'.length)}/${artifactId}`
  }
  return `${JITPACK_BASE_URL}/#/${artifactId}`
}

/**
 * Returns a human-browsable URL for the given artifact in its authoritative registry.
 */
export function resolveRegistryPageUrl(groupId: string, artifactId: string): string {
  const entry = findEntry(groupId)
  if (entry) return entry.pageUrl(groupId, artifactId)
  return `https://central.sonatype.com/artifact/${groupId}/${artifactId}`
}

/**
 * Like resolveRegistryPageUrl, but uses the resolved base URL to determine which
 * registry page to show. When the artifact was actually fetched from Maven Central
 * (e.g. via the Central fallback after the primary alternative registry 404'd), the
 * namespace-based routing would point at the wrong registry page — this overrides it.
 */
export function resolveRegistryPageUrlFromBase(
  groupId: string,
  artifactId: string,
  resolvedBaseUrl: string,
): string {
  const isCentral =
    resolvedBaseUrl === MAVEN_CENTRAL_BASE_URL ||
    resolvedBaseUrl === process.env.MAVEN_FETCHER_BASE_URL
  if (isCentral) {
    return `https://central.sonatype.com/artifact/${groupId}/${artifactId}`
  }
  if (resolvedBaseUrl === GRADLE_PLUGIN_PORTAL_BASE_URL) {
    return `${GRADLE_PLUGIN_PORTAL_BASE_URL}/${groupId.replace(/\./g, '/')}/${artifactId}/`
  }
  if (resolvedBaseUrl === JITPACK_BASE_URL) {
    return jitpackPageUrl(groupId, artifactId)
  }
  return resolveRegistryPageUrl(groupId, artifactId)
}
