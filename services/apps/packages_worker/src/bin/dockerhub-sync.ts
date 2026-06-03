import { getServiceLogger } from '@crowd/logging'

import { getDockerhubConfig } from '../config'
import { getPackagesDb } from '../db'
import { runDockerhubLoop } from '../dockerhub'

const log = getServiceLogger()

let shuttingDown = false

const shutdown = async () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down dockerhub-sync...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('dockerhub-sync starting...')

  const config = getDockerhubConfig()

  if (config.tokens.length === 0) {
    log.error('ENRICHER_GITHUB_TOKENS is required (comma-separated PATs)')
    process.exit(1)
  }

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  log.info(
    { tokens: config.tokens.length, batchSize: config.batchSize, hubBaseUrl: config.hubBaseUrl },
    'Starting dockerhub loop',
  )

  await runDockerhubLoop(qx, config, () => shuttingDown)

  log.info('dockerhub-sync stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'dockerhub-sync fatal error')
  process.exit(1)
})
