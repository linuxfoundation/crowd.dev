import { getServiceChildLogger } from '@crowd/logging'

import { countTiedPackageRepos, rescoreAllPackageRepos } from './rescore'

const log = getServiceChildLogger('packageRepoConfidenceSweep')

export async function sweepPackageRepoConfidenceScores(): Promise<number> {
  const appliedRows = await rescoreAllPackageRepos()
  log.info({ appliedRows }, 'Package repo confidence sweep complete')
  return appliedRows
}

export async function assertNoTiedPackageRepos(): Promise<void> {
  const tiedPackages = await countTiedPackageRepos()
  if (tiedPackages > 0) {
    throw new Error(
      `package_repos confidence uniqueness violated: ${tiedPackages} package(s) have repo links sharing a confidence`,
    )
  }
  log.info('No tied package repo confidence values')
}
