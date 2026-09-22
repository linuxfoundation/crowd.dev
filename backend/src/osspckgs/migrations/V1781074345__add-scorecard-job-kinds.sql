-- Extend osspckgs_ingest_jobs.job_kind CHECK constraint to include scorecard kinds.
-- Required for ingestScorecard workflow (CM-1227).
ALTER TABLE osspckgs_ingest_jobs
  DROP CONSTRAINT osspckgs_ingest_jobs_job_kind_check,
  ADD CONSTRAINT osspckgs_ingest_jobs_job_kind_check CHECK (job_kind IN (
    'packages', 'versions', 'package_dependencies',
    'repos', 'package_repos',
    'advisories', 'advisory_packages',
    'dependent_counts',
    'scorecard_repos', 'scorecard_checks'
  ));
