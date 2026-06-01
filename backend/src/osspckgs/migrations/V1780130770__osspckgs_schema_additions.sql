-- 1. Add dependent_counts job kind
ALTER TABLE osspckgs_ingest_jobs
    DROP CONSTRAINT osspckgs_ingest_jobs_job_kind_check;

ALTER TABLE osspckgs_ingest_jobs
    ADD CONSTRAINT osspckgs_ingest_jobs_job_kind_check
    CHECK (job_kind IN (
        'packages', 'versions', 'package_dependencies',
        'repos', 'package_repos',
        'advisories', 'advisory_packages',
        'dependent_counts'
    ));

-- 2. Staging schema (used by gcsParquetToStaging activity)
CREATE SCHEMA IF NOT EXISTS staging;

-- 3. Staging + merge row count columns
ALTER TABLE osspckgs_ingest_jobs
    ADD COLUMN IF NOT EXISTS row_count_staging bigint,
    ADD COLUMN IF NOT EXISTS table_row_counts  jsonb;

COMMENT ON COLUMN osspckgs_ingest_jobs.row_count_staging IS 'Rows loaded into staging table from GCS parquet files';
COMMENT ON COLUMN osspckgs_ingest_jobs.row_count_pg       IS 'Total rows inserted into final table(s) after merge';
COMMENT ON COLUMN osspckgs_ingest_jobs.table_row_counts   IS 'Per-table inserted row counts, e.g. {"packages": 5000000}';

-- 4. BQ job telemetry columns
--    bq_job_id: GCP job ID for direct lookup in BigQuery Studio
--    bq_stats:  JSONB bag with bytesProcessed/Billed, slotMs, cacheHit, durationMs, referencedTables, etc.
ALTER TABLE osspckgs_ingest_jobs
    ADD COLUMN IF NOT EXISTS bq_job_id text,
    ADD COLUMN IF NOT EXISTS bq_stats  jsonb;

COMMENT ON COLUMN osspckgs_ingest_jobs.bq_job_id IS 'GCP BigQuery job ID (project:location.jobId)';
COMMENT ON COLUMN osspckgs_ingest_jobs.bq_stats   IS 'Full BQ job statistics: bytesProcessed, bytesBilled, slotMs, cacheHit, durationMs, referencedTables, outputRows, etc.';

CREATE INDEX ON osspckgs_ingest_jobs (bq_job_id) WHERE bq_job_id IS NOT NULL;

-- 5. Estimated BQ cost ($5/TB on-demand pricing)
--    bq_bytes_billed stores totalBytesProcessed; billing rounds up to 10 MB min per table.
ALTER TABLE osspckgs_ingest_jobs
    ADD COLUMN IF NOT EXISTS bq_cost_usd numeric(12, 8) GENERATED ALWAYS AS (
        ROUND(COALESCE(bq_bytes_billed, 0)::numeric / 1000000000000.0 * 5.0, 8)
    ) STORED;

COMMENT ON COLUMN osspckgs_ingest_jobs.bq_cost_usd IS 'Estimated BQ cost in USD at $5/TB (bq_bytes_billed / 1 TB * $5)';

-- 6. Named exports: group multiple job-kind exports under a user-chosen label.
--    Enables pre-exporting data to GCS outside of a bootstrap run, then triggering
--    bootstrap with --export-name to skip all BQ and load directly from GCS.
ALTER TABLE osspckgs_ingest_jobs
    ADD COLUMN IF NOT EXISTS export_name text;

COMMENT ON COLUMN osspckgs_ingest_jobs.export_name IS 'Named export group (e.g. "cargo-may-2026"). Jobs created via exportToBucket script are tagged here; bootstrap uses this to find pre-exported GCS data and skip BQ.';

CREATE INDEX ON osspckgs_ingest_jobs (job_kind, export_name) WHERE export_name IS NOT NULL;

-- 7. Denormalize namespace+name onto versions for fast deps merge resolution.
--    Allows resolving (ecosystem, namespace, name, number) → version_id in one index lookup
--    instead of joining packages first. Critical for large staging tables (NPM has ~160M rows).
ALTER TABLE versions
    ADD COLUMN IF NOT EXISTS namespace text,
    ADD COLUMN IF NOT EXISTS name     text;

UPDATE versions v
SET namespace = p.namespace,
    name      = p.name
FROM packages p
WHERE p.id = v.package_id;

ALTER TABLE versions ALTER COLUMN name SET NOT NULL;

CREATE INDEX ON versions (ecosystem, COALESCE(namespace, ''), name, number);

-- 8. Drop redundant non-unique index on advisory_affected_ranges(advisory_package_id).
--    The UNIQUE index on (advisory_package_id, COALESCE(introduced_version,''), COALESCE(fixed_version,''))
--    already serves prefix lookups on advisory_package_id — the non-unique index is dead weight.
DROP INDEX IF EXISTS advisory_affected_ranges_advisory_package_id_idx;
