#!/usr/bin/env tsx

/**
 * Recompute package_repos.confidence with package_repo_confidence()
 * (see V1788307200). Used for the initial backfill and as the daily sweep that
 * catches repo-state changes the enricher's inline rescore missed.
 *
 * Usage:
 *   pnpm rescore-package-repos [--chunk-size <n>] [--check-only]
 *
 *   --chunk-size <n>  Rows per committed chunk (default: 25000).
 *   --check-only      Skip the rescore, only report packages with tied scores.
 */
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

const log = getServiceChildLogger('rescorePackageRepos')

const DEFAULT_CHUNK_SIZE = 25000

async function reportTies(): Promise<number> {
  const qx = await getPackagesDb()
  const row = await qx.selectOne(`
    SELECT COUNT(*)::int AS tied_packages
    FROM (
      SELECT package_id
      FROM package_repos
      GROUP BY package_id
      HAVING COUNT(*) <> COUNT(DISTINCT confidence)
    ) t
  `)
  return row.tied_packages as number
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  const chunkIdx = args.indexOf('--chunk-size')
  const chunkSize = chunkIdx !== -1 ? Number(args[chunkIdx + 1]) : DEFAULT_CHUNK_SIZE
  if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
    throw new Error(`--chunk-size must be a positive integer, got: ${args[chunkIdx + 1]}`)
  }

  if (!args.includes('--check-only')) {
    const qx = await getPackagesDb()
    log.info({ chunkSize }, 'Rescoring package_repos')
    // The procedure COMMITs per chunk, so it must not run inside a transaction.
    const row = await qx.selectOne(
      `CALL rescore_package_repo_confidence(NULL::bigint[], $(chunkSize), 0)`,
      { chunkSize },
    )
    log.info({ appliedRows: row.applied_rows }, 'Rescore complete')
  }

  const tiedPackages = await reportTies()
  if (tiedPackages > 0) {
    log.warn({ tiedPackages }, 'Packages with tied confidence (repo_id DESC breaks ties deterministically)')
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
