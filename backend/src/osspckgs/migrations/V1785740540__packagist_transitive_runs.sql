-- Packagist transitive-dependents lane: run-level state ledger.
--
-- The lane is a whole-ecosystem batch (snapshot → closure → keyset merge), so its
-- natural unit of record is a run
--
-- One row per run (weekly cadence + manual triggers). A 'pending' row is reused
-- across Temporal retries of the prepare activity; 'merging' rows either finish
-- ('done') or are marked 'failed' by the workflow's terminal error handling.
CREATE TABLE packagist_transitive_runs (
  id                        serial      PRIMARY KEY,
  status                    text        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'merging', 'done', 'failed')),
  edge_count                bigint,     -- distinct package-level direct edges snapshotted
  packages_with_dependents  bigint,     -- rows in the closure output
  processed_rows            bigint,     -- packagist packages visited by the merge drain
  changed_rows              bigint,     -- rows whose transitive_dependent_count actually changed
  error_message             text,
  started_at                timestamptz NOT NULL DEFAULT now(),
  finished_at               timestamptz
);
