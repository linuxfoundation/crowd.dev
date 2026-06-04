import { getServiceLogger } from '@crowd/logging'

import { getEnricherConfig, getGithubAppConfig } from '../config'
import { getPackagesDb } from '../db'
import { fetchRateLimitDiagnostics, resolveInstallations } from '../enricher/githubAppAuth'
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

  const enricherConfig = getEnricherConfig()
  const appConfig = getGithubAppConfig()

  const installationIds = await resolveInstallations(appConfig)

  if (installationIds.length === 0) {
    log.error('No GitHub App installations found — cannot build token pool')
    process.exit(1)
  }

  await fetchRateLimitDiagnostics(appConfig.appId, appConfig.privateKeyPem, installationIds)

  log.info(
    {
      installations: installationIds.length,
      concurrency: enricherConfig.concurrency,
      fetchTimeoutMs: enricherConfig.fetchTimeoutMs,
    },
    'Starting enrichment loop',
  )

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  await runEnrichmentLoop(qx, installationIds, appConfig, enricherConfig, () => shuttingDown)

  log.info('github-repos-enricher stopped.')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'github-repos-enricher fatal error')
  process.exit(1)
})
