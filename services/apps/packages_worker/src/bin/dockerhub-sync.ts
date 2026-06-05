import { getServiceLogger } from '@crowd/logging'

import { getDockerhubConfig, getGithubAppConfig } from '../config'
import { getPackagesDb } from '../db'
import { runDockerhubLoop } from '../dockerhub'
import { fetchRateLimitDiagnostics, resolveInstallations } from '../enricher/githubAppAuth'

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
  const appConfig = getGithubAppConfig()

  const installationIds = await resolveInstallations(appConfig)

  if (installationIds.length === 0) {
    log.error('No GitHub App installations found — cannot build token pool')
    process.exit(1)
  }

  await fetchRateLimitDiagnostics(appConfig.appId, appConfig.privateKeyPem, installationIds)

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  log.info(
    {
      installations: installationIds.length,
      batchSize: config.batchSize,
      hubBaseUrl: config.hubBaseUrl,
    },
    'Starting dockerhub loop',
  )

  await runDockerhubLoop(qx, installationIds, appConfig, config, () => shuttingDown)

  log.info('dockerhub-sync stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'dockerhub-sync fatal error')
  process.exit(1)
})
