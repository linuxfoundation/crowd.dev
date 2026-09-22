
CREATE TABLE pypi_package_state (
  purl                      text        PRIMARY KEY,
  metadata_first_scanned_at timestamptz NOT NULL DEFAULT now(),
  metadata_last_run_at      timestamptz,
  metadata_run_result       jsonb         -- { status, attempts, httpStatus?, errorKind?, message? }
);

CREATE INDEX ON pypi_package_state (metadata_last_run_at);
