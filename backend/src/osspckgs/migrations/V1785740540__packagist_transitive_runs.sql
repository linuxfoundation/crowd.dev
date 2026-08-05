-- Run ledger for the packagist transitive-dependents lane: one row per run;
-- prepare retries reuse 'pending', and 'merging' rows end 'done' or 'failed'.
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
