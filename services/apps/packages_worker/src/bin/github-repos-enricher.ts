import { getServiceLogger } from '@crowd/logging'

import { getEnricherConfig } from '../config'
import { getPackagesDb } from '../db'
import { runEnrichmentLoop } from '../enricher/runEnrichmentLoop'

const log = getServiceLogger()

let shuttingDown = false

const shutdown = async () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down github-repos-enricher...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('github-repos-enricher starting...')

  const config = getEnricherConfig()

  if (config.tokens.length === 0) {
    log.error('ENRICHER_GITHUB_TOKENS is required (comma-separated PATs)')
    process.exit(1)
  }

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  log.info(
    { tokens: config.tokens.length, batchSize: config.batchSize },
    'Starting enrichment loop',
  )

  await runEnrichmentLoop(qx, config, () => shuttingDown)

  log.info('github-repos-enricher stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'github-repos-enricher fatal error')
  process.exit(1)
})
