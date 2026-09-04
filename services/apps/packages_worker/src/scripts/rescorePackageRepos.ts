#!/usr/bin/env tsx

/**
 * Recompute package_repos.confidence with package_repo_confidence()
 * (see V1788307200). Used for the initial backfill; the recurring sweep runs as the
 * package-repo-confidence-sweep-daily Temporal schedule.
 *
 * Usage:
 *   pnpm rescore-package-repos [--chunk-size <n>] [--check-only]
 *
 *   --chunk-size <n>  Rows per committed chunk (default: 25000).
 *   --check-only      Skip the rescore, only report packages with tied scores.
 */
import { getServiceChildLogger } from '@crowd/logging'

import {
  DEFAULT_RESCORE_CHUNK_SIZE,
  countTiedPackageRepos,
  rescoreAllPackageRepos,
} from '../package-repos/rescore'

const log = getServiceChildLogger('rescorePackageRepos')

async function main(): Promise {
  const args = process.argv.slice(2)
  const chunkIdx = args.indexOf('--chunk-size')
  const chunkSize = chunkIdx !== -1 ? Number(args[chunkIdx + 1]) : DEFAULT_RESCORE_CHUNK_SIZE
  if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
    throw new Error(`--chunk-size must be a positive integer, got: ${args[chunkIdx + 1]}`)
  }

  if (!args.includes('--check-only')) {
    log.info({ chunkSize }, 'Rescoring package_repos')
    const appliedRows = await rescoreAllPackageRepos(chunkSize)
    log.info({ appliedRows }, 'Rescore complete')
  }

  const tiedPackages = await countTiedPackageRepos()
  if (tiedPackages > 0) {
    log.warn(
      { tiedPackages },
      'Packages with tied confidence (repo_id DESC breaks ties deterministically)',
    )
  } else {
    log.info('No tied confidence values')
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error(err, 'Fatal error')
    process.exit(1)
  })
