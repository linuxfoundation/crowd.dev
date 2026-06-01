import { QueryExecutor } from '../queryExecutor'

export async function markNpmPackageScanned(qx: QueryExecutor, name: string): Promise<void> {
  await qx.result(
    `INSERT INTO npm_package_state (name) VALUES ($(name)) ON CONFLICT (name) DO NOTHING`,
    { name },
  )
}

export async function getScannedNpmPackages(qx: QueryExecutor, names: string[]): Promise<string[]> {
  const rows: Array<{ name: string }> = await qx.select(
    `SELECT name FROM npm_package_state WHERE name = ANY($(names)::text[])`,
    { names },
  )
  return rows.map((r) => r.name)
}
