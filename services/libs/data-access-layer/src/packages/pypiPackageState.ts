import { QueryExecutor } from '../queryExecutor'

// Structured outcome of a metadata ingest run, stored as JSONB in
// pypi_package_state.metadata_run_result.
export interface PypiMetadataRunResult {
  status: 'success' | 'error'
  attempts: number
  httpStatus?: number
  errorKind?: string
  message?: string
}

export async function markPypiPackageScanned(
  qx: QueryExecutor,
  purl: string,
  result: PypiMetadataRunResult,
): Promise<void> {
  await qx.result(
    `INSERT INTO pypi_package_state (purl, metadata_run_result, metadata_last_run_at)
     VALUES ($(purl), $(result)::jsonb, NOW())
     ON CONFLICT (purl) DO UPDATE SET
       metadata_run_result  = EXCLUDED.metadata_run_result,
       metadata_last_run_at = EXCLUDED.metadata_last_run_at`,
    { purl, result: JSON.stringify(result) },
  )
}

// PyPI packages in `packages` that still need a metadata run: either never scanned
// (no state row) or stale (last run older than refreshDays). PyPI has no _changes-style
// feed like npm, so freshness is driven by this staleness window instead. When
// onlyCritical is true (the intended steady state) enrichment is scoped to is_critical
// packages like npm/maven; pass false to sweep the whole PyPI set (temporary — e.g.
// while criticality is still being populated).
export async function getUnscannedPypiPurls(
  qx: QueryExecutor,
  afterPurl: string,
  batchSize: number,
  refreshDays: number,
  onlyCritical: boolean,
): Promise<string[]> {
  const rows: Array<{ purl: string }> = await qx.select(
    `SELECT p.purl
       FROM packages p
       LEFT JOIN pypi_package_state s ON s.purl = p.purl
      WHERE p.ecosystem = 'pypi'
        AND (NOT $(onlyCritical) OR p.is_critical = TRUE)
        AND p.purl > $(afterPurl)
        AND (
          s.metadata_last_run_at IS NULL
          OR s.metadata_last_run_at < NOW() - ($(refreshDays) || ' days')::interval
        )
      ORDER BY p.purl
      LIMIT $(batchSize)`,
    { afterPurl, batchSize, refreshDays, onlyCritical },
  )
  return rows.map((r) => r.purl)
}
