import { QueryExecutor } from '../queryExecutor'

// Structured outcome of a metadata ingest run, stored as JSONB in
// npm_package_state.metadata_run_result.
export interface NpmMetadataRunResult {
  status: 'success' | 'error'
  attempts: number
  httpStatus?: number
  errorKind?: string
  message?: string
}

// Mark a package as metadata-scanned and record the run outcome (+ timestamp). Keyed
// by purl (from the packages row). metadata_run_result/metadata_last_run_at are refreshed
// on every run — metadata_last_run_at is the authoritative "metadata has run" signal.
export async function markNpmPackageScanned(
  qx: QueryExecutor,
  purl: string,
  result: NpmMetadataRunResult,
): Promise<void> {
  await qx.result(
    `INSERT INTO npm_package_state (purl, metadata_run_result, metadata_last_run_at)
     VALUES ($(purl), $(result)::jsonb, NOW())
     ON CONFLICT (purl) DO UPDATE SET
       metadata_run_result  = EXCLUDED.metadata_run_result,
       metadata_last_run_at = EXCLUDED.metadata_last_run_at`,
    { purl, result: JSON.stringify(result) },
  )
}

// Critical npm packages in the `packages` table whose metadata has never been scanned.
// Only is_critical packages are enriched — metadata is deep, per-package work, so it is
// scoped to the critical set (matching the daily-downloads pass).
// Keyset-paginated on purl so the workflow can drain a large first run across
// many continueAsNew runs.
export async function getUnscannedNpmPurls(
  qx: QueryExecutor,
  afterPurl: string,
  batchSize: number,
): Promise<string[]> {
  const rows: Array<{ purl: string }> = await qx.select(
    `SELECT p.purl
       FROM packages p
       LEFT JOIN npm_package_state s ON s.purl = p.purl
      WHERE p.ecosystem = 'npm'
        AND p.is_critical = TRUE
        AND p.purl > $(afterPurl)
        AND s.metadata_last_run_at IS NULL
      ORDER BY p.purl
      LIMIT $(batchSize)`,
    { afterPurl, batchSize },
  )
  return rows.map((r) => r.purl)
}

// Given a list of changed npm registry names (from the _changes feed), return the
// purls of the critical ones that exist as npm rows in `packages`. The purl is read
// straight from the row; feed names are matched against the decoded namespace/name
// columns (the purl is percent-encoded, so substr(purl) would be %40scope/name).
// Restricted to is_critical packages — non-critical packages are never metadata-scanned,
// so there is nothing to keep fresh when they change.
export async function getNpmPurlsForChangedNames(
  qx: QueryExecutor,
  names: string[],
): Promise<string[]> {
  if (names.length === 0) return []
  const rows: Array<{ purl: string }> = await qx.select(
    `SELECT p.purl
       FROM packages p
       JOIN unnest($(names)::text[]) AS u(name)
         ON (CASE WHEN p.namespace IS NOT NULL THEN p.namespace || '/' || p.name ELSE p.name END) = u.name
      WHERE p.ecosystem = 'npm'
        AND p.is_critical = TRUE`,
    { names },
  )
  return rows.map((r) => r.purl)
}
