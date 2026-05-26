export function getPackagesDbConfig() {
  return {
    host: process.env.CROWD_PACKAGES_DB_WRITE_HOST,
    port: parseInt(process.env.CROWD_PACKAGES_DB_PORT, 10),
    database: process.env.CROWD_PACKAGES_DB_DATABASE,
    user: process.env.CROWD_PACKAGES_DB_USERNAME,
    password: process.env.CROWD_PACKAGES_DB_PASSWORD,
  }
}

export function getEnricherConfig() {
  const rawTokens = process.env.ENRICHER_GITHUB_TOKENS ?? ''
  const tokens = rawTokens.split(',').map((t) => t.trim()).filter(Boolean)

  return {
    tokens,
    pageSize: parseInt(process.env.PAGE_SIZE, 10),
    batchSize: parseInt(process.env.BATCH_SIZE, 10),
    maxRetries: parseInt(process.env.MAX_RETRIES, 10),
    updateIntervalHours: parseInt(process.env.UPDATE_INTERVAL_HOURS, 10),
    idleSleepSec: parseInt(process.env.IDLE_SLEEP_SEC, 10),
  }
}
