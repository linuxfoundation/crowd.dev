import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const RETRY = {
  initialInterval: '30 seconds',
  backoffCoefficient: 2,
  maximumAttempts: 3,
}

const { cargoDownloadAndLoad } = proxyActivities<typeof activities>({
  startToCloseTimeout: '90 minutes',
  retry: RETRY,
})

const {
  cargoEnrichPackages,
  cargoEnrichVersions,
  cargoEnrichRepos,
  cargoEnrichMaintainers,
  cargoEnrichDownloadsDaily,
  cargoFlushAudit,
  cargoCleanup,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: RETRY,
})

// Sequential: phases touch disjoint tables; flushAudit before cleanup.
export async function cargoSyncWorkflow(): Promise<void> {
  const load = await cargoDownloadAndLoad()
  log.info('cargoSync loaded dump', { ...load })

  const packages = await cargoEnrichPackages()
  const versions = await cargoEnrichVersions()
  const repos = await cargoEnrichRepos()
  const maintainers = await cargoEnrichMaintainers()
  const downloads = await cargoEnrichDownloadsDaily()
  const auditRows = await cargoFlushAudit()
  await cargoCleanup()

  log.info('cargoSync complete', {
    matched: load.matched,
    packages,
    versions,
    repos,
    maintainers,
    downloads,
    auditRows,
  })
}
