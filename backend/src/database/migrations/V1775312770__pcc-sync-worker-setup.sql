-- Add maturity field to segments for PCC project_maturity_level sync
ALTER TABLE segments ADD COLUMN IF NOT EXISTS maturity TEXT NULL;

-- Catch-all table for PCC sync issues that require manual review
CREATE TABLE IF NOT EXISTS pcc_projects_sync_errors (
  id                    BIGSERIAL PRIMARY KEY,
  run_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  external_project_id   TEXT,
  external_project_slug TEXT,
  error_type            TEXT NOT NULL,
  details               JSONB,
  resolved              BOOLEAN NOT NULL DEFAULT FALSE
);

-- Deduplication index: one unresolved error per (project, error_type).
-- On repeated daily exports the same error upserts in place instead of accumulating rows.
-- Excludes rows where external_project_id IS NULL (e.g. SCHEMA_MISMATCH with no project id).
CREATE UNIQUE INDEX IF NOT EXISTS pcc_sync_errors_dedup_idx
  ON pcc_projects_sync_errors (external_project_id, error_type)
  WHERE NOT resolved AND external_project_id IS NOT NULL;

-- Deduplication index for unidentifiable rows (no external_project_id).
-- Keyed on (error_type, reason) so repeated daily exports don't accumulate duplicate rows
-- for the same class of malformed input (e.g. rows missing PROJECT_ID/NAME/DEPTH).
CREATE UNIQUE INDEX IF NOT EXISTS pcc_sync_errors_dedup_unknown_idx
  ON pcc_projects_sync_errors (error_type, (details->>'reason'))
  WHERE NOT resolved AND external_project_id IS NULL;
