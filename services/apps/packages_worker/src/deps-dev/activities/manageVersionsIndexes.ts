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
]

export async function dropVersionsIndexes(): Promise<{ dropped: string[]; droppedConstraint: string | null }> {
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

export async function rebuildVersionsIndexes(): Promise<{ rebuilt: string[]; rebuiltConstraint: boolean }> {
  const qx = await getPackagesDb()

  const existing: Array<{ indexdef: string }> = await qx.select(NON_CONSTRAINT_INDEXES_SQL)
  const existingDefs = existing.map((r) => r.indexdef.toLowerCase())

  const rebuilt: string[] = []

  for (const idx of SECONDARY_INDEXES) {
    const alreadyExists = existingDefs.some((def) => def.includes(idx.matchString ?? `(${idx.columns})`))
    if (alreadyExists) {
      log.info({ columns: idx.columns }, 'Index already exists, skipping')
      continue
    }
    log.info({ columns: idx.columns }, 'Creating index on versions')
    await qx.result(idx.createSql)
    rebuilt.push(idx.columns)
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
