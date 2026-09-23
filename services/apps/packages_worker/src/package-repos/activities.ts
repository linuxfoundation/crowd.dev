import { getServiceChildLogger } from '@crowd/logging'

import { countTiedPackageRepos, rescoreAllPackageRepos } from './rescore'

const log = getServiceChildLogger('packageRepoConfidenceSweep')

export async function sweepPackageRepoConfidenceScores(): Promise<number> {
  const appliedRows = await rescoreAllPackageRepos()
  log.info({ appliedRows }, 'Package repo confidence sweep complete')
  return appliedRows
}

export async function reportTiedPackageRepos(): Promise<number> {
  const tiedPackages = await countTiedPackageRepos()
  if (tiedPackages > 0) {
    log.warn({ tiedPackages }, 'Packages with repo links sharing a confidence')
  }
  return tiedPackages
}
