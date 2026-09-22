
-- Packagist worker: per-lane ingestion state tracking (metadata, downloads-30d, daily downloads)
CREATE TABLE packagist_package_state (
  purl                        text        PRIMARY KEY,
  first_seen_at               timestamptz NOT NULL DEFAULT now(),
  metadata_last_run_at        timestamptz,
  metadata_run_result         jsonb,         -- { status, attempts, httpStatus?, errorKind?, message? }
  metadata_last_modified      text,          -- Last-Modified from p2 endpoint, replayed as If-Modified-Since
  downloads_30d_last_run_at   timestamptz,
  downloads_30d_run_result    jsonb,         -- { status, attempts, httpStatus?, errorKind?, message? }
  daily_downloads_last_run_at timestamptz,
  daily_downloads_run_result  jsonb          -- { status, attempts, httpStatus?, errorKind?, message? }
);

CREATE INDEX ON packagist_package_state (metadata_last_run_at);
CREATE INDEX ON packagist_package_state (downloads_30d_last_run_at);
CREATE INDEX ON packagist_package_state (daily_downloads_last_run_at);
