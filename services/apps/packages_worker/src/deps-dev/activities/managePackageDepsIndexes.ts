import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('managePackageDepsIndexes')

// Secondary indexes on package_dependencies — safe to drop before full-load INSERT and rebuild after.
// The PRIMARY KEY (id, depends_on_id) stays. The UNIQUE (version_id, depends_on_id, dependency_kind)
// is also dropped/rebuilt for full loads — dropping it removes the per-row B-tree probe on INSERT,
// which is the dominant cost when loading 1B+ rows. The full-load INSERT uses plain INSERT (no ON
// CONFLICT) while the constraint is absent. If BQ data has duplicates the ADD CONSTRAINT will fail,
// which tells us we need BQ-level dedup first.
const UNIQUE_CONSTRAINT_SQL = `
  SELECT conname
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  WHERE t.relname = 'package_dependencies'
    AND c.contype = 'u'
  LIMIT 1
`

// Only secondary (non-constraint) indexes — PK excluded, UNIQUE excluded (handled separately above).
const NON_CONSTRAINT_INDEXES_SQL = `
  SELECT i.indexname, i.indexdef
  FROM pg_indexes i
  WHERE i.tablename = 'package_dependencies'
    AND NOT EXISTS (
      SELECT 1 FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid AND t.relname = 'package_dependencies'
      JOIN pg_class ix ON ix.oid = c.conindid AND ix.relname = i.indexname
      WHERE c.contype IN ('p', 'u')
    )
`

const SECONDARY_INDEXES: Array<{ columns: string; createSql: string }> = [
  {
    columns: 'depends_on_id, depends_on_version_id',
    createSql: `CREATE INDEX ON package_dependencies (depends_on_id, depends_on_version_id)`,
  },
  {
    columns: 'version_id',
    createSql: `CREATE INDEX ON package_dependencies (version_id)`,
  },
]

// Drops secondary (non-constraint) indexes on package_dependencies.
// Idempotent: DROP INDEX IF EXISTS on each found index.
// For a partitioned table, dropping the parent index cascades to all 64 partitions.
export async function dropPackageDepsIndexes(): Promise<{ dropped: string[]; droppedConstraint: string | null }> {
  const qx = await getPackagesDb()

  const rows: Array<{ indexname: string }> = await qx.select(NON_CONSTRAINT_INDEXES_SQL)
  const names = rows.map((r: { indexname: string }) => r.indexname)

  if (names.length > 0) {
    log.info({ indexes: names }, 'Dropping secondary indexes on package_dependencies')
    for (const name of names) {
      await qx.result(`DROP INDEX IF EXISTS ${name}`)
    }
  } else {
    log.info('No secondary indexes found — already dropped or never created')
  }

  // Also drop the UNIQUE constraint so full-load INSERTs skip the per-row B-tree probe.
  // Plain INSERT (no ON CONFLICT) is used while the constraint is absent.
  const constraintRow = await qx.selectOneOrNone(UNIQUE_CONSTRAINT_SQL)
  let droppedConstraint: string | null = null
  if (constraintRow) {
    log.info({ constraint: constraintRow.conname }, 'Dropping UNIQUE constraint on package_dependencies')
    await qx.result(`ALTER TABLE package_dependencies DROP CONSTRAINT ${constraintRow.conname}`)
    droppedConstraint = constraintRow.conname
  } else {
    log.info('UNIQUE constraint not found — already dropped')
  }

  log.info({ indexes: names, droppedConstraint }, 'Drop phase complete')
  return { dropped: names, droppedConstraint }
}

// Rebuilds secondary indexes on package_dependencies.
// Idempotent: existence check uses exact column-list match — "(version_id)" — to avoid
// false positives from substring matches (e.g. "depends_on_version_id" contains "version_id").
// Only non-constraint indexes are checked so the UNIQUE constraint cannot cause a false positive.
export async function rebuildPackageDepsIndexes(): Promise<{ rebuilt: string[]; rebuiltConstraint: boolean }> {
  const qx = await getPackagesDb()

  const existing: Array<{ indexdef: string }> = await qx.select(NON_CONSTRAINT_INDEXES_SQL)
  const existingDefs = existing.map((r: { indexdef: string }) => r.indexdef.toLowerCase())

  const rebuilt: string[] = []

  for (const idx of SECONDARY_INDEXES) {
    const alreadyExists = existingDefs.some((def: string) => def.includes(`(${idx.columns})`))
    if (alreadyExists) {
      log.info({ columns: idx.columns }, 'Index already exists, skipping')
      continue
    }
    log.info({ columns: idx.columns }, 'Creating index on package_dependencies')
    await qx.result(idx.createSql)
    rebuilt.push(idx.columns)
  }

  // Remove cross-chunk duplicates before rebuilding the UNIQUE constraint.
  // DISTINCT ON deduplicates within a single chunk, but the same (root, dep) pair can appear in
  // multiple BQ parquet files (different chunks), each inserting its own row. The DELETE here is
  // a one-time scan at the end of the full load — cheaper than failing the constraint rebuild.
  // depends_on_id is included in the USING join so PG routes each delete to the correct partition.
  //
  // Run per partition (depends_on_id % 64 = p) so each iteration prunes to one of the 64
  // partitions instead of scanning all 1.15B rows at once. Same total rows read; each pass
  // fits in work_mem and avoids cross-partition sort.
  const NUM_PARTITIONS = 64
  let totalDedupDeleted = 0
  for (let p = 0; p < NUM_PARTITIONS; p++) {
    const result = await qx.result(`
      DELETE FROM package_dependencies pd
      USING (
        SELECT id, depends_on_id
        FROM (
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
    totalDedupDeleted += result
  }
  log.info({ rowsDeleted: totalDedupDeleted }, 'Cross-chunk duplicate rows removed from package_dependencies')

  const constraintRow = await qx.selectOneOrNone(UNIQUE_CONSTRAINT_SQL)
  let rebuiltConstraint = false
  if (!constraintRow) {
    log.info('Rebuilding UNIQUE constraint on package_dependencies (this may take a while on large tables)')
    await qx.result(
      `ALTER TABLE package_dependencies ADD UNIQUE (version_id, depends_on_id, dependency_kind)`,
    )
    rebuiltConstraint = true
    log.info('UNIQUE constraint rebuilt')
  } else {
    log.info({ constraint: constraintRow.conname }, 'UNIQUE constraint already exists, skipping')
  }

  log.info({ rebuilt, rebuiltConstraint }, 'Rebuild phase complete')
  return { rebuilt, rebuiltConstraint }
}
