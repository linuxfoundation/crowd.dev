ALTER TABLE member_contributor_jobs
    DROP CONSTRAINT member_contributor_jobs_collection_path_check,
    ADD CONSTRAINT member_contributor_jobs_collection_path_check
        CHECK (collection_path IN ('github', 'llm_repo_resolution')),
    DROP CONSTRAINT member_contributor_jobs_status_check,
    ADD CONSTRAINT member_contributor_jobs_status_check
        CHECK (status IN ('pending', 'running', 'done', 'failed', 'unresolvable')),
    ADD COLUMN repo_url          TEXT,
    ADD COLUMN repo_url_source   TEXT CHECK (repo_url_source IN ('llm_inferred')),
    ADD COLUMN llm_confidence    NUMERIC(3, 2),
    ADD COLUMN llm_input_tokens  INTEGER,
    ADD COLUMN llm_output_tokens INTEGER,
    ADD COLUMN llm_cost_usd      NUMERIC(10, 6),
    ADD COLUMN submission_count  INTEGER NOT NULL DEFAULT 1;
