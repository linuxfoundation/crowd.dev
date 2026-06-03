import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('manageVersionsConstraints')

// FK constraints on versions — dropped before full-load INSERT and rebuilt after.
// Dropping FKs eliminates the per-row trigger checks (referential integrity lookups against
// packages) on every inserted row. Safe to drop because the INSERT...SELECT JOINs against
// packages guarantee only valid rows are inserted.
// On Oracle Cloud managed PostgreSQL, session_replication_role=replica is blocked even with
// the REPLICATION role granted, so this is the only way to skip FK trigger overhead.
// Only fetch FKs referencing parent tables (not partition children) — see managePackageDepsConstraints.ts
// for the full explanation. versions only has one FK (→ packages, not partitioned) so this filter
// is a no-op here, but kept for consistency and future safety.
const FK_CONSTRAINTS_SQL = `
  SELECT conname,
         pg_get_constraintdef(oid) AS condef
  FROM pg_constraint
  WHERE conrelid = 'versions'::regclass
    AND contype = 'f'
    AND NOT EXISTS (
      SELECT 1 FROM pg_inherits WHERE inhrelid = confrelid
    )
`

// Known FK definitions — used to recreate constraints after full load.
// Matches the migration V1779710880__initial_schema.sql.
const FK_DEFINITIONS = [
  {
    columns: 'package_id',
    sql: `ALTER TABLE versions ADD FOREIGN KEY (package_id) REFERENCES packages (id)`,
  },
]

export async function dropVersionsConstraints(): Promise<{ dropped: string[] }> {
  const qx = await getPackagesDb()

  const rows: Array<{ conname: string; condef: string }> = await qx.select(FK_CONSTRAINTS_SQL)

  if (rows.length === 0) {
    log.info('No FK constraints found on versions — already dropped or never created')
    return { dropped: [] }
  }

  log.info({ constraints: rows.map((r) => r.conname) }, 'Dropping FK constraints on versions')

  for (const row of rows) {
    await qx.result(`ALTER TABLE versions DROP CONSTRAINT IF EXISTS "${row.conname}"`)
  }

  log.info({ dropped: rows.map((r) => r.conname) }, 'FK constraints dropped')
  return { dropped: rows.map((r) => r.conname) }
}

export async function rebuildVersionsConstraints(): Promise<{ rebuilt: string[] }> {
  const qx = await getPackagesDb()

  const existing: Array<{ conname: string; condef: string }> = await qx.select(FK_CONSTRAINTS_SQL)

  const rebuilt: string[] = []

  for (const def of FK_DEFINITIONS) {
    const alreadyExists = existing.some((r) => r.condef.includes(`FOREIGN KEY (${def.columns})`))
    if (alreadyExists) {
      log.info({ columns: def.columns }, 'FK constraint already exists, skipping')
      continue
    }
    log.info({ columns: def.columns }, 'Rebuilding FK constraint on versions (validates all rows)')
    await qx.result(def.sql)
    rebuilt.push(def.columns)
  }

  log.info({ rebuilt }, 'FK constraints rebuilt')
  return { rebuilt }
}
