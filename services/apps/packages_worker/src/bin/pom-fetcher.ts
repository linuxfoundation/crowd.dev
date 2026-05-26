import { getServiceLogger } from '@crowd/logging'

import { getPomFetcherConfig } from '../config'
import { getPackagesDb } from '../db'
import { runPomEnrichmentLoop } from '../pom-fetcher/runPomEnrichmentLoop'

const log = getServiceLogger()

let shuttingDown = false

const shutdown = async () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down pom-fetcher...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('pom-fetcher starting...')

  const config = getPomFetcherConfig()
  log.info(
    { batchSize: config.batchSize, concurrency: config.concurrency, staleDays: config.staleDays },
    'Config loaded',
  )

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  await runPomEnrichmentLoop(qx, config, () => shuttingDown)

  log.info('pom-fetcher stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'pom-fetcher fatal error')
  process.exit(1)
})
