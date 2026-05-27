import { getServiceLogger } from '@crowd/logging'

import { getOsvConfig } from '../config'
import { getPackagesDb } from '../db'
import { runOsvSync } from '../osv'

const log = getServiceLogger()

let shuttingDown = false

const shutdown = async () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down osv-sync...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('osv-sync starting...')

  const config = getOsvConfig()

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  log.info(
    {
      ecosystems: config.ecosystems,
      syncIntervalHours: config.syncIntervalHours,
      batchSize: config.batchSize,
    },
    'Starting OSV sync loop',
  )

  await runOsvSync(qx, config, () => shuttingDown)

  log.info('osv-sync stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'osv-sync fatal error')
  process.exit(1)
})
