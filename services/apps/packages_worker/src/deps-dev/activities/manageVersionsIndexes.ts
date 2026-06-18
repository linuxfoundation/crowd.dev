import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('manageVersionsIndexes')

const UNIQUE_CONSTRAINT_SQL = `
  SELECT conname
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  WHERE t.relname = 'versions'
    AND c.contype = 'u'
  LIMIT 1
`

// Only secondary (non-constraint) indexes on the parent versions table.
// DROP INDEX on the parent cascades to all 32 partitions.
const NON_CONSTRAINT_INDEXES_SQL = `
  SELECT i.indexname, i.indexdef
  FROM pg_indexes i
  WHERE i.tablename = 'versions'
    AND NOT EXISTS (
      SELECT 1 FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid AND t.relname = 'versions'
      JOIN pg_class ix ON ix.oid = c.conindid AND ix.relname = i.indexname
      WHERE c.contype IN ('p', 'u')
    )
`

const SECONDARY_INDEXES: Array<{ columns: string; createSql: string; matchString?: string }> = [
  {
    columns: 'published_at desc',
    createSql: `CREATE INDEX ON versions (published_at DESC)`,
  },
  {
    columns: 'package_id',
    createSql: `CREATE INDEX ON versions (package_id) WHERE is_latest`,
    // Include WHERE clause so the check doesn't match a hypothetical plain (package_id) index.
    matchString: '(package_id) where is_latest',
  },
  {
    columns: "ecosystem, coalesce(namespace, ''), name, number",
    createSql: `CREATE INDEX ON versions (ecosystem, COALESCE(namespace, ''), name, number)`,
    matchString: "(ecosystem, coalesce(namespace, ''), name, number)",
  },
]

export async function dropVersionsIndexes(): Promise<{
  dropped: string[]
  droppedConstraint: string | null
}> {
  const qx = await getPackagesDb()

  const rows: Array<{ indexname: string }> = await qx.select(NON_CONSTRAINT_INDEXES_SQL)
  const names = rows.map((r) => r.indexname)

  if (names.length > 0) {
    log.info({ indexes: names }, 'Dropping secondary indexes on versions')
    for (const name of names) {
      await qx.result(`DROP INDEX IF EXISTS ${name}`)
    }
  } else {
    log.info('No secondary indexes found — already dropped or never created')
  }

  const constraintRow = await qx.selectOneOrNone(UNIQUE_CONSTRAINT_SQL)
  let droppedConstraint: string | null = null
  if (constraintRow) {
    log.info({ constraint: constraintRow.conname }, 'Dropping UNIQUE constraint on versions')
    await qx.result(`ALTER TABLE versions DROP CONSTRAINT ${constraintRow.conname}`)
    droppedConstraint = constraintRow.conname
  } else {
    log.info('UNIQUE constraint not found — already dropped')
  }

  log.info({ indexes: names, droppedConstraint }, 'Drop phase complete')
  return { dropped: names, droppedConstraint }
}

export async function rebuildVersionsIndexes(): Promise<{
  rebuilt: string[]
  rebuiltConstraint: boolean
}> {
  const qx = await getPackagesDb()

  const existing: Array<{ indexdef: string }> = await qx.select(NON_CONSTRAINT_INDEXES_SQL)
  const existingDefs = existing.map((r) => r.indexdef.toLowerCase())

  const toRebuild = SECONDARY_INDEXES.filter(
    (idx) => !existingDefs.some((def) => def.includes(idx.matchString ?? `(${idx.columns})`)),
  )
  const rebuilt: string[] = []

  // Build indexes in parallel — each needs its own connection.
  // maintenance_work_mem per connection: with 32 partitions and default 64MB, PG spills to
  // disk on every partition; 2GB lets the sort fit in RAM and cuts build time dramatically.
  // Build indexes in parallel — each on its own connection so they run concurrently.
  await Promise.all(
    toRebuild.map(async (idx) => {
      const conn = await getPackagesDb()
      log.info({ columns: idx.columns }, 'Creating index on versions')
      await conn.result(idx.createSql)
      rebuilt.push(idx.columns)
    }),
  )

  // Remove cross-chunk duplicates before rebuilding the UNIQUE constraint.
  // versions is HASH-partitioned by package_id (32 partitions). Loop over each partition table
  // directly so PG scans one partition at a time rather than all 32 at once.
  const NUM_PARTITIONS = 32
  let totalDedupDeleted = 0
  for (let p = 0; p < NUM_PARTITIONS; p++) {
    const table = `versions_p${p}`
    const deleted = await qx.result(`
      DELETE FROM ${table} v
      USING (
        SELECT id
        FROM (
          SELECT id,
                 ROW_NUMBER() OVER (PARTITION BY package_id, number ORDER BY id) AS rn
          FROM ${table}
        ) sub
        WHERE rn > 1
      ) dupes
      WHERE v.id = dupes.id
    `)
    totalDedupDeleted += deleted
  }
  if (totalDedupDeleted > 0) {
    log.info({ rowsDeleted: totalDedupDeleted }, 'Cross-chunk duplicate rows removed from versions')
  }

  const constraintRow = await qx.selectOneOrNone(UNIQUE_CONSTRAINT_SQL)
  let rebuiltConstraint = false
  if (!constraintRow) {
    log.info('Rebuilding UNIQUE constraint on versions (this may take a while on large tables)')
    await qx.result(`ALTER TABLE versions ADD UNIQUE (package_id, number)`)
    rebuiltConstraint = true
    log.info('UNIQUE constraint rebuilt')
  } else {
    log.info({ constraint: constraintRow.conname }, 'UNIQUE constraint already exists, skipping')
  }

  log.info({ rebuilt, rebuiltConstraint }, 'Rebuild phase complete')
  return { rebuilt, rebuiltConstraint }
}
