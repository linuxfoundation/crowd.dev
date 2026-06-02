import { getServiceLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'
import { runMavenEnrichmentLoop } from '../maven/runMavenEnrichmentLoop'

const log = getServiceLogger()

let shuttingDown = false

const shutdown = async () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down maven...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('maven starting...')

  const config = getMavenConfig()
  log.info(config, 'Config loaded')

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  await runMavenEnrichmentLoop(qx, config, () => shuttingDown)

  log.info('maven stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'maven fatal error')
  process.exit(1)
})
