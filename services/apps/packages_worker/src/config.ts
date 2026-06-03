function requireEnv(name: string): string {
  const val = process.env[name]
  if (!val) throw new Error(`Missing required environment variable: ${name}`)
  return val
}

function requireEnvInt(name: string): number {
  return parseInt(requireEnv(name), 10)
}

export function getPackagesDbConfig() {
  return {
    host: requireEnv('CROWD_PACKAGES_DB_WRITE_HOST'),
    port: requireEnvInt('CROWD_PACKAGES_DB_PORT'),
    database: requireEnv('CROWD_PACKAGES_DB_DATABASE'),
    user: requireEnv('CROWD_PACKAGES_DB_USERNAME'),
    password: requireEnv('CROWD_PACKAGES_DB_PASSWORD'),
  }
}

export function getGithubAppConfig() {
  const rawPrivateKey = requireEnv('CROWD_GITHUB_PRIVATE_KEY')
  const privateKeyPem = Buffer.from(rawPrivateKey, 'base64').toString('ascii')

  const rawIds = process.env.ENRICHER_GITHUB_INSTALLATION_IDS ?? ''
  const installationIdOverrides = rawIds
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => parseInt(s, 10))
    .filter((n) => !isNaN(n))

  return {
    appId: requireEnv('CROWD_GITHUB_APP_ID'),
    privateKeyPem,
    installationIdOverrides,
  }
}

export function getEnricherConfig() {
  return {
    updateIntervalHours: requireEnvInt('ENRICHER_REPO_UPDATE_INTERVAL_HOURS'),
    idleSleepSec: requireEnvInt('ENRICHER_IDLE_SLEEP_SEC'),
    concurrency: parseInt(process.env.ENRICHER_CONCURRENCY ?? '80', 10),
    fetchTimeoutMs: parseInt(process.env.ENRICHER_FETCH_TIMEOUT_MS ?? '10000', 10),
  }
}

// Which source drives the critical Maven sync:
//   'maven' → poll packages_universe by staleness (current behaviour, default/fallback)
//   'api'   → only enrich what our delta feed reports as changed
//   'both'  → run both passes in the same Temporal tick
export type MavenSyncSource = 'api' | 'maven' | 'both'

function parseMavenSyncSource(raw: string | undefined): MavenSyncSource {
  if (raw === 'api' || raw === 'both') return raw
  // Anything else (unset, typo, legacy value) falls back to the current behaviour.
  return 'maven'
}

export function getMavenConfig() {
  const syncSource = parseMavenSyncSource(process.env.MAVEN_SYNC_SOURCE)
  const deltaApiBaseUrl = (process.env.MAVEN_DELTA_API_URL ?? '').replace(/\/+$/, '')

  if (syncSource !== 'maven' && !deltaApiBaseUrl) {
    throw new Error(`MAVEN_SYNC_SOURCE='${syncSource}' requires MAVEN_DELTA_API_URL to be set`)
  }

  return {
    batchSize: requireEnvInt('POM_FETCHER_BATCH_SIZE'),
    concurrency: requireEnvInt('POM_FETCHER_CONCURRENCY'),
    nonCriticalBatchSize: requireEnvInt('POM_FETCHER_NON_CRITICAL_BATCH_SIZE'),
    nonCriticalConcurrency: requireEnvInt('POM_FETCHER_NON_CRITICAL_CONCURRENCY'),
    refreshDays: requireEnvInt('POM_FETCHER_REFRESH_DAYS'),
    groupDelayMs: requireEnvInt('POM_FETCHER_GROUP_DELAY_MS'),
    syncSource,
    deltaApi: {
      baseUrl: deltaApiBaseUrl,
      token: process.env.MAVEN_DELTA_API_TOKEN || undefined,
      pageSize: process.env.MAVEN_DELTA_API_PAGE_SIZE
        ? parseInt(process.env.MAVEN_DELTA_API_PAGE_SIZE, 10)
        : 100,
      lookbackMinutes: process.env.MAVEN_DELTA_API_LOOKBACK_MINUTES
        ? parseInt(process.env.MAVEN_DELTA_API_LOOKBACK_MINUTES, 10)
        : 15,
      includePrerelease: process.env.MAVEN_DELTA_API_INCLUDE_PRERELEASE === 'true',
    },
  }
}
