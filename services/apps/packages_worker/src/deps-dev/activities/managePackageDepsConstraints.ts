import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('managePackageDepsConstraints')

// FK constraints on package_dependencies — dropped before full-load INSERT and rebuilt after.
// Dropping FKs eliminates the per-row trigger checks (referential integrity lookups against
// packages/versions) on every inserted row. Safe to drop because the INSERT...SELECT JOINs
// against packages/versions guarantee only valid rows are inserted.
// On Oracle Cloud managed PostgreSQL, session_replication_role=replica is blocked even with
// the REPLICATION role granted, so this is the only way to skip FK trigger overhead.
// Only fetch FKs referencing parent tables (not partition children).
// Oracle Cloud PG creates per-partition FK constraints automatically when a FK references a
// partitioned table — one on the parent (e.g. → versions) plus one per partition (→ versions_p0
// … versions_p31). Dropping the parent FK cascades and removes all partition-level FKs.
// Filtering out partition-referencing FKs (those whose confrelid has a parent in pg_inherits)
// avoids trying to drop already-cascaded constraints and hitting "does not exist" errors.
const FK_CONSTRAINTS_SQL = `
  SELECT conname,
         pg_get_constraintdef(oid) AS condef
  FROM pg_constraint
  WHERE conrelid = 'package_dependencies'::regclass
    AND contype = 'f'
    AND NOT EXISTS (
      SELECT 1 FROM pg_inherits WHERE inhrelid = confrelid
    )
`

// Known FK definitions — used to recreate constraints after full load.
// Matches the migration V1779710880__initial_schema.sql.
const FK_DEFINITIONS = [
  {
    columns: 'depends_on_id',
    sql: `ALTER TABLE package_dependencies ADD FOREIGN KEY (depends_on_id) REFERENCES packages (id)`,
  },
  {
    columns: 'version_id, package_id',
    sql: `ALTER TABLE package_dependencies ADD FOREIGN KEY (version_id, package_id) REFERENCES versions (id, package_id)`,
  },
  {
    columns: 'depends_on_version_id, depends_on_id',
    sql: `ALTER TABLE package_dependencies ADD FOREIGN KEY (depends_on_version_id, depends_on_id) REFERENCES versions (id, package_id)`,
  },
]

export async function dropPackageDepsConstraints(): Promise<{ dropped: string[] }> {
  const qx = await getPackagesDb()

  const rows: Array<{ conname: string; condef: string }> = await qx.select(FK_CONSTRAINTS_SQL)

  if (rows.length === 0) {
    log.info('No FK constraints found on package_dependencies — already dropped or never created')
    return { dropped: [] }
  }

  log.info(
    { constraints: rows.map((r) => r.conname) },
    'Dropping FK constraints on package_dependencies',
  )

  for (const row of rows) {
    await qx.result(`ALTER TABLE package_dependencies DROP CONSTRAINT IF EXISTS ${row.conname}`)
  }

  log.info({ dropped: rows.map((r) => r.conname) }, 'FK constraints dropped')
  return { dropped: rows.map((r) => r.conname) }
}

export async function rebuildPackageDepsConstraints(): Promise<{ rebuilt: string[] }> {
  const qx = await getPackagesDb()

  const existing: Array<{ conname: string }> = await qx.select(FK_CONSTRAINTS_SQL)

  const rebuilt: string[] = []

  for (const def of FK_DEFINITIONS) {
    const alreadyExists = existing.some((r) => r.conname.includes(def.columns.split(',')[0].trim()))
    if (alreadyExists) {
      log.info({ columns: def.columns }, 'FK constraint already exists, skipping')
      continue
    }
    log.info(
      { columns: def.columns },
      'Rebuilding FK constraint on package_dependencies (validates all rows)',
    )
    await qx.result(def.sql)
    rebuilt.push(def.columns)
  }

  log.info({ rebuilt }, 'FK constraints rebuilt')
  return { rebuilt }
}
