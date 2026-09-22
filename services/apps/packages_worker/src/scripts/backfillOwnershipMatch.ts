#!/usr/bin/env tsx

/**
 * One-time backfill of package_repos.ownership_match (CM-1394) for 'declared' rows
 * still on the default 'no_evidence' — see V1789034800. Only reads packages/repos/
 * maintainers already in the DB, no registry re-crawl.
 *
 * confidence is not touched here; the existing package-repo-confidence-sweep-daily
 * schedule picks up the new ownership_match value on its next run.
 *
 * Usage:
 *   pnpm --filter @crowd/packages-worker backfill-ownership-match [--chunk-size <n>]
 *
 *   --chunk-size <n>  Rows per committed chunk (default: 25000).
 */
import { getServiceChildLogger } from '@crowd/logging'

import {
  DEFAULT_OWNERSHIP_MATCH_BACKFILL_CHUNK_SIZE,
  backfillAllPackageRepoOwnershipMatch,
} from '../package-repos/backfillOwnershipMatch'

const log = getServiceChildLogger('backfillOwnershipMatch')

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  const chunkIdx = args.indexOf('--chunk-size')
  const chunkSize =
    chunkIdx !== -1 ? Number(args[chunkIdx + 1]) : DEFAULT_OWNERSHIP_MATCH_BACKFILL_CHUNK_SIZE
  if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
    throw new Error(`--chunk-size must be a positive integer, got: ${args[chunkIdx + 1]}`)
  }

  log.info({ chunkSize }, 'Backfilling package_repos.ownership_match')
  const appliedRows = await backfillAllPackageRepoOwnershipMatch(chunkSize)
  log.info({ appliedRows }, 'Backfill complete')
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error(err, 'Fatal error')
    process.exit(1)
  })
