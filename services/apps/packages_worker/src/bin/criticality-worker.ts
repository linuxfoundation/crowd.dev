import { getServiceLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

const log = getServiceLogger()

let shuttingDown = false

process.on('SIGINT',  () => { shuttingDown = true })
process.on('SIGTERM', () => { shuttingDown = true })

async function main() {
  log.info('criticality-worker starting')

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db. Ready — trigger PageRank via run:pagerank or impact score via rank_packages_universe().')

  while (!shuttingDown) {
    await new Promise(resolve => setTimeout(resolve, 5_000))
  }

  log.info('criticality-worker stopped')
  process.exit(0)
}

main().catch(err => {
  log.error({ err }, 'criticality-worker fatal error')
  process.exit(1)
})
