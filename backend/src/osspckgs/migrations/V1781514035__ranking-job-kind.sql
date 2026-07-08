-- Drop CHECK constraints on job_kind and sync_mode so new kinds can be added
-- without a migration every time.
ALTER TABLE osspckgs_ingest_jobs
  DROP CONSTRAINT osspckgs_ingest_jobs_job_kind_check,
  DROP CONSTRAINT osspckgs_ingest_jobs_sync_mode_check;
