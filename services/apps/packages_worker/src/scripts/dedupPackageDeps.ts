#!/usr/bin/env tsx

/**
 * Dedup cross-chunk duplicate rows from package_dependencies, then rebuild
 * the UNIQUE constraint (version_id, depends_on_id, dependency_kind).
 *
 * Run after a full-load where the constraint was dropped before bulk INSERT.
 *
 * Usage:
 *   pnpm dedup-package-deps [--concurrency <n>] [--dry-run]
 *
 *   --concurrency <n>  Partitions to process in parallel (default: 8).
 *   --dry-run          Count duplicates without deleting.
 */
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

const log = getServiceChildLogger('dedupPackageDeps')

const NUM_PARTITIONS = 64

async function processPartition(p: number, dryRun: boolean): Promise<number> {
  log.info({ partition: p }, dryRun ? 'counting partition' : 'deduping partition')
  const conn = await getPackagesDb()
  const count = await conn.tx(async (tx) => {
    await tx.result(`SET LOCAL work_mem = '2GB'`)
    if (dryRun) {
      const row = await tx.selectOne(`
        SELECT COUNT(*) AS cnt
        FROM (
          SELECT ROW_NUMBER() OVER (
                   PARTITION BY version_id, depends_on_id, dependency_kind
                   ORDER BY id
                 ) AS rn
          FROM package_dependencies
          WHERE depends_on_id % ${NUM_PARTITIONS} = ${p}
        ) sub
        WHERE rn > 1
      `)
      return Number(row.cnt)
    }
    return tx.result(`
      DELETE FROM package_dependencies pd
      USING (
        SELECT id, depends_on_id FROM (
          SELECT id, depends_on_id,
                 ROW_NUMBER() OVER (
                   PARTITION BY version_id, depends_on_id, dependency_kind
                   ORDER BY id
                 ) AS rn
          FROM package_dependencies
          WHERE depends_on_id % ${NUM_PARTITIONS} = ${p}
        ) sub
        WHERE rn > 1
      ) dupes
      WHERE pd.id = dupes.id AND pd.depends_on_id = dupes.depends_on_id
    `)
  })
  log.info({ partition: p, count }, dryRun ? 'partition counted' : 'partition done')
  return count
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  const concurrencyIdx = args.indexOf('--concurrency')
  const concurrency = concurrencyIdx !== -1 ? Number(args[concurrencyIdx + 1]) : 8
  const dryRun = args.includes('--dry-run')

  if (dryRun) log.info('DRY RUN — counting only')
  log.info({ concurrency, numPartitions: NUM_PARTITIONS }, 'Starting dedup')

  let total = 0
  for (let batch = 0; batch < NUM_PARTITIONS; batch += concurrency) {
    const partitions = Array.from(
      { length: Math.min(concurrency, NUM_PARTITIONS - batch) },
      (_, i) => batch + i,
    )
    const counts = await Promise.all(partitions.map((p) => processPartition(p, dryRun)))
    total += counts.reduce((s, n) => s + n, 0)
    log.info({ partitions, total }, dryRun ? 'batch counted' : 'batch done')
  }

  log.info({ total }, dryRun ? 'duplicates found' : 'dedup complete')

  if (dryRun) return

  const qx = await getPackagesDb()
  const existing = await qx.selectOneOrNone(`
    SELECT conname FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'package_dependencies' AND c.contype = 'u'
    LIMIT 1
  `)
  if (existing) {
    log.info({ constraint: existing.conname }, 'UNIQUE constraint already exists')
  } else {
    log.info('Rebuilding UNIQUE constraint')
    await qx.result(
      `ALTER TABLE package_dependencies ADD UNIQUE (version_id, depends_on_id, dependency_kind)`,
    )
    log.info('UNIQUE constraint rebuilt')
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error(err, 'Fatal error')
    process.exit(1)
  })
