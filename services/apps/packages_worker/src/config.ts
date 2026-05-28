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

export function getPomFetcherConfig() {
  return {
    // critical packages — HTTP-bound, keep low
    batchSize: parseInt(process.env.POM_FETCHER_BATCH_SIZE ?? '50', 10),
    concurrency: parseInt(process.env.POM_FETCHER_CONCURRENCY ?? '3', 10),
    fullRefreshDays: parseInt(process.env.POM_FETCHER_FULL_REFRESH_DAYS ?? '90', 10),
    // non-critical packages — DB-only, can go much higher
    nonCriticalBatchSize: parseInt(process.env.POM_FETCHER_NON_CRITICAL_BATCH_SIZE ?? '500', 10),
    nonCriticalConcurrency: parseInt(process.env.POM_FETCHER_NON_CRITICAL_CONCURRENCY ?? '20', 10),
    nonCriticalRefreshDays: parseInt(process.env.POM_FETCHER_NON_CRITICAL_REFRESH_DAYS ?? '180', 10),
    idleSleepSec: parseInt(process.env.POM_FETCHER_IDLE_SLEEP_SEC ?? '3600', 10),
  }
}
