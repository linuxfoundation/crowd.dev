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

export function getCdpDbConfig() {
  return {
    host: requireEnv('CROWD_DB_READ_HOST'),
    port: requireEnvInt('CROWD_DB_PORT'),
    database: requireEnv('CROWD_DB_DATABASE'),
    user: requireEnv('CROWD_DB_USERNAME'),
    password: requireEnv('CROWD_DB_PASSWORD'),
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
    concurrency: parseInt(process.env.ENRICHER_CONCURRENCY ?? '150', 10),
    fetchTimeoutMs: parseInt(process.env.ENRICHER_FETCH_TIMEOUT_MS ?? '10000', 10),
  }
}

export function getSecurityContactsConfig() {
  return {
    // Sent on all registry calls; crates.io rejects requests without an identifying UA.
    userAgent: requireEnv('SECURITY_CONTACTS_USER_AGENT'),
  }
}

export function getMavenConfig() {
  return {
    batchSize: requireEnvInt('MAVEN_FETCHER_BATCH_SIZE'),
    concurrency: requireEnvInt('MAVEN_FETCHER_CONCURRENCY'),
    nonCriticalBatchSize: requireEnvInt('MAVEN_FETCHER_NON_CRITICAL_BATCH_SIZE'),
    nonCriticalConcurrency: requireEnvInt('MAVEN_FETCHER_NON_CRITICAL_CONCURRENCY'),
    refreshDays: requireEnvInt('MAVEN_FETCHER_REFRESH_DAYS'),
    groupDelayMs: requireEnvInt('MAVEN_FETCHER_GROUP_DELAY_MS'),
  }
}

export function getCargoConfig() {
  return {
    dumpUrl: process.env.CARGO_DUMP_URL ?? 'https://static.crates.io/db-dump.tar.gz',
  }
}

export function getGoConfig() {
  return {
    fetchTimeoutMs: parseInt(process.env.GO_FETCH_TIMEOUT_MS ?? '15000', 10),
    proxyConcurrency: Math.max(1, parseInt(process.env.GO_PROXY_CONCURRENCY ?? '10', 10)),
  }
}

export function getNuGetConfig() {
  return {
    batchSize: parseInt(process.env.NUGET_FETCHER_BATCH_SIZE ?? '1000', 10),
    concurrency: parseInt(process.env.NUGET_FETCHER_CONCURRENCY ?? '20', 10),
    groupDelayMs: parseInt(process.env.NUGET_FETCHER_GROUP_DELAY_MS ?? '0', 10),
    isCritical: (process.env.NUGET_FETCHER_IS_CRITICAL ?? 'false') === 'true',
  }
}

export function getRubyGemsConfig() {
  return {
    batchSize: parseInt(process.env.RUBYGEMS_FETCHER_BATCH_SIZE ?? '10000', 10),
    concurrency: parseInt(process.env.RUBYGEMS_FETCHER_CONCURRENCY ?? '8', 10),
  }
}

export function getRubyGemsCriticalConfig() {
  return {
    batchSize: parseInt(process.env.RUBYGEMS_CRITICAL_FETCHER_BATCH_SIZE ?? '5000', 10),
    concurrency: parseInt(process.env.RUBYGEMS_CRITICAL_FETCHER_CONCURRENCY ?? '4', 10),
  }
}

export function getDockerhubConfig() {
  return {
    hubBaseUrl: requireEnv('DOCKERHUB_API_BASE_URL'),
    batchSize: requireEnvInt('DOCKERHUB_BATCH_SIZE'),
    refreshIntervalHours: requireEnvInt('DOCKERHUB_REFRESH_INTERVAL_HOURS'),
    discoveryIntervalDays: requireEnvInt('DOCKERHUB_DISCOVERY_INTERVAL_DAYS'),
    idleSleepSec: requireEnvInt('DOCKERHUB_IDLE_SLEEP_SEC'),
  }
}
