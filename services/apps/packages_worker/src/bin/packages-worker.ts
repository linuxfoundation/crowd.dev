import fs from 'fs'
import path from 'path'

import { getServiceLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

const log = getServiceLogger()

const liveFilePath = path.join(__dirname, '../tmp/packages-worker-live.tmp')
const readyFilePath = path.join(__dirname, '../tmp/packages-worker-ready.tmp')

let shuttingDown = false

const shutdown = async () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down packages-worker...')
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('packages-worker starting...')

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  // Create tmp directory for health probe files
  fs.mkdirSync(path.dirname(liveFilePath), { recursive: true })

  setInterval(async () => {
    if (shuttingDown) return
    try {
      await Promise.all([
        fs.promises.open(liveFilePath, 'a').then((f) => f.close()),
        fs.promises.open(readyFilePath, 'a').then((f) => f.close()),
      ])
    } catch (err) {
      log.warn({ err }, 'Failed to write health probe files')
    }
  }, 5000)

  log.info('packages-worker started, idle.')
}

main().catch((err) => {
  log.error({ err }, 'packages-worker fatal error')
  process.exit(1)
})
