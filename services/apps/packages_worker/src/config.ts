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

export function getEnricherConfig() {
  const rawTokens = process.env.ENRICHER_GITHUB_TOKENS ?? ''
  const tokens = rawTokens
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)

  return {
    tokens,
    batchSize: requireEnvInt('ENRICHER_BATCH_SIZE'),
    updateIntervalHours: requireEnvInt('ENRICHER_REPO_UPDATE_INTERVAL_HOURS'),
    idleSleepSec: requireEnvInt('ENRICHER_IDLE_SLEEP_SEC'),
  }
}

export function getOsvConfig() {
  const ecosystems = requireEnv('OSV_ECOSYSTEMS')
    .split(',')
    .map((e) => e.trim())
    .filter(Boolean)

  return {
    bulkBaseUrl: requireEnv('OSV_BULK_BASE_URL'),
    ecosystems,
    syncIntervalHours: requireEnvInt('OSV_SYNC_INTERVAL_HOURS'),
    idleSleepSec: requireEnvInt('OSV_IDLE_SLEEP_SEC'),
    tmpDir: requireEnv('OSV_TMP_DIR'),
    batchSize: requireEnvInt('OSV_BATCH_SIZE'),
    deriveBatchSize: requireEnvInt('OSV_DERIVE_BATCH_SIZE'),
  }
}
