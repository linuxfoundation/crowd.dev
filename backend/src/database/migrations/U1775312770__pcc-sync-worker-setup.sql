DROP INDEX IF EXISTS pcc_sync_errors_dedup_idx;

DROP TABLE IF EXISTS pcc_projects_sync_errors;

ALTER TABLE segments DROP COLUMN IF EXISTS maturity;
