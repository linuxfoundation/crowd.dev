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
  const tokens = rawTokens.split(',').map((t) => t.trim()).filter(Boolean)

  return {
    tokens,
    batchSize: requireEnvInt('ENRICHER_BATCH_SIZE'),
    updateIntervalHours: requireEnvInt('ENRICHER_REPO_UPDATE_INTERVAL_HOURS'),
    idleSleepSec: requireEnvInt('ENRICHER_IDLE_SLEEP_SEC'),
  }
}
