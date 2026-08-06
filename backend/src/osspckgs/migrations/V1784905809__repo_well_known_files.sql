-- Well-known files inventory per repo, populated by the github-repos-enricher.
CREATE TABLE IF NOT EXISTS repo_well_known_files (
    id bigserial PRIMARY KEY,
    repo_id bigint NOT NULL REFERENCES repos (id) ON DELETE CASCADE,
    file_type text NOT NULL, -- security | contributing | governance | maintainers | code_of_conduct | readme
    directory text NOT NULL, -- root | .github | docs
    path text NOT NULL,
    blob_oid text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT NOW(),
    -- observation time, not commit time; bumped on content change, disappearance, reappearance
    change_detected_at timestamptz NOT NULL DEFAULT NOW(),
    checked_at timestamptz NOT NULL,
    deleted_at timestamptz,
    UNIQUE (repo_id, path)
);

CREATE INDEX IF NOT EXISTS repo_well_known_files_change_detected_at_idx
    ON repo_well_known_files (change_detected_at);

CREATE INDEX IF NOT EXISTS repo_well_known_files_file_type_idx
    ON repo_well_known_files (file_type)
    WHERE deleted_at IS NULL;
