-- Add created_at / updated_at to all packages-db tables for Tinybird sync watermarking.
--
-- Excluded tables and why:
--   package_name_history          — event log; has changed_at
--   audit_field_changes           — audit log; has logged_at
--   osspckgs_ingest_jobs          — job tracking; has started_at / finished_at
--   npm_worker_state              — state machine; has updated_at
--   npm_package_state             — watermark columns serve the same purpose
--   npm_package_universe_state    — watermark columns serve the same purpose
--   package_criticality_spotlight — override config; has added_at
--
-- repos: created_at already exists (stores the GitHub repository creation date) and
-- last_synced_at serves as the row-level updated_at. Neither column is added here.
--
-- packages, versions, repo_docker: last_synced_at already serves as updated_at.
-- Only created_at is added to avoid duplicate semantics.
--
-- package_repos: verified_at already serves as updated_at. Only created_at is added.
--
-- Partitioned tables (versions, package_dependencies, downloads_daily, downloads_last_30d):
-- PostgreSQL 12+ propagates new columns to all child partitions automatically —
-- no per-partition ALTER needed.

ALTER TABLE packages
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE package_funding_links
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE versions
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE package_dependencies
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE repo_scorecard_checks
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE repo_docker
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE package_repos
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE advisories
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE advisory_packages
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE advisory_affected_ranges
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE maintainers
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE package_maintainers
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE downloads_daily
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE downloads_last_30d
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();
