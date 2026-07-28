-- Blast-radius analysis pipeline (CM-1328). Mirrors the 4-stage PoC
-- (linux-foundation/blast-radius): intel -> dependents -> reachability -> report.
-- Everything the PoC kept as JSON files under data/<VULN-ID>/ moves to these
-- tables so runs are resumable, queryable, and inspectable across ecosystems.
-- The aggregated report (naive vs true blast radius, bucket counts) is not
-- materialized here — it's derived from blast_radius_verdicts at read time.

-- One row per submitted job (backend's submitBlastRadiusJob). id matches the
-- analysisId the API already generates before starting the Temporal workflow.
CREATE TABLE IF NOT EXISTS blast_radius_analyses (
    id                              UUID PRIMARY KEY,
    -- Raw GHSA/CVE id from the request; the advisory may not be synced from
    -- deps.dev BQ yet, so advisory_id is resolved (and filled in) later.
    advisory_osv_id                 TEXT NOT NULL,
    advisory_id                     BIGINT REFERENCES advisories (id),
    -- Raw value from the request (purl or bare name); may not resolve to a
    -- known package yet. package_id is filled in once/if it resolves.
    package_name                    TEXT,
    package_id                      BIGINT REFERENCES packages (id),
    ecosystem                       TEXT NOT NULL,
    status                          TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'done', 'failed')),
    force                           BOOLEAN NOT NULL DEFAULT FALSE,
    -- Stage 2 (dependents) run metadata — the per-dependent rows live in
    -- blast_radius_dependents; these are the population-level numbers report
    -- rendering needs alongside them (dependents_doc.source / candidates_considered).
    dependents_source               TEXT,
    candidates_considered           INT,
    total_cost_usd                  NUMERIC(10, 4) NOT NULL DEFAULT 0,
    error                           TEXT,
    started_at                      TIMESTAMPTZ,
    completed_at                    TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS blast_radius_analyses_advisory_id_idx
    ON blast_radius_analyses (advisory_id);
CREATE INDEX IF NOT EXISTS blast_radius_analyses_package_id_idx
    ON blast_radius_analyses (package_id) WHERE package_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS blast_radius_analyses_status_idx
    ON blast_radius_analyses (status);

-- Stage 1 output — one row per analysis (symbol_spec.json equivalent).
CREATE TABLE IF NOT EXISTS blast_radius_symbol_specs (
    id                       BIGSERIAL PRIMARY KEY,
    analysis_id              UUID NOT NULL UNIQUE REFERENCES blast_radius_analyses (id),
    vuln_id                  TEXT NOT NULL,
    aliases                  TEXT[],
    package                  TEXT NOT NULL,
    ecosystem                TEXT NOT NULL,
    affected_ranges          JSONB NOT NULL,
    vulnerable_versions      TEXT[] NOT NULL,
    analyzed_version         TEXT NOT NULL,
    related_affected_packages TEXT[],
    vulnerable_symbols       JSONB NOT NULL, -- [{name, kind, defined_in, exported_as[], notes}]
    import_signatures        JSONB NOT NULL, -- {style: [signature, ...]}
    exploit_preconditions    TEXT,
    reachability_notes       TEXT,
    confidence               NUMERIC(3, 2) NOT NULL CHECK (confidence BETWEEN 0.00 AND 1.00),
    sources                  TEXT[],
    summary                  TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stage 2 output — one row per candidate dependent, analyzed or excluded.
-- Unifies dependents.json's "analyzed" and "excluded_by_range" lists: the
-- excluded ones never got a resolved version/tarball, hence those columns
-- are nullable.
CREATE TABLE IF NOT EXISTS blast_radius_dependents (
    id                     BIGSERIAL PRIMARY KEY,
    analysis_id             UUID NOT NULL REFERENCES blast_radius_analyses (id),
    package_id              BIGINT REFERENCES packages (id),
    name                    TEXT NOT NULL,
    version                 TEXT,
    downloads               BIGINT,
    declared_range          TEXT,
    dependency_kind         TEXT, -- 'dependencies' | 'peerDependencies' | 'optionalDependencies'
    range_includes_vuln     BOOLEAN,
    range_check             TEXT, -- 'matched' | 'excluded' | 'unparseable-included'
    tarball_url             TEXT,
    excluded_by_range       BOOLEAN NOT NULL DEFAULT FALSE,
    exclusion_reason        TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (analysis_id, name)
);

CREATE INDEX IF NOT EXISTS blast_radius_dependents_analysis_id_idx
    ON blast_radius_dependents (analysis_id);
CREATE INDEX IF NOT EXISTS blast_radius_dependents_package_id_idx
    ON blast_radius_dependents (package_id) WHERE package_id IS NOT NULL;

-- Stage 3 output — one reachability verdict per analyzed (non-excluded) dependent.
CREATE TABLE IF NOT EXISTS blast_radius_verdicts (
    id                       BIGSERIAL PRIMARY KEY,
    analysis_id              UUID NOT NULL REFERENCES blast_radius_analyses (id),
    dependent_id             BIGINT NOT NULL UNIQUE REFERENCES blast_radius_dependents (id),
    uses_package             BOOLEAN NOT NULL,
    imports_vulnerable_symbol BOOLEAN NOT NULL,
    import_style             TEXT, -- 'main-member' | 'deep-import' | 'standalone-pkg' | 'reexport' | 'none'
    reachable_verdict        TEXT NOT NULL CHECK (reachable_verdict IN ('affected', 'not_affected', 'unclear')),
    confidence               NUMERIC(3, 2) NOT NULL CHECK (confidence BETWEEN 0.00 AND 1.00),
    evidence                 JSONB, -- [{file, line, snippet}]
    reasoning                TEXT,
    model                    TEXT,
    turns_used               INT,
    cost_usd                 NUMERIC(10, 4) NOT NULL DEFAULT 0,
    error                    TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS blast_radius_verdicts_analysis_id_idx
    ON blast_radius_verdicts (analysis_id);
CREATE INDEX IF NOT EXISTS blast_radius_verdicts_analysis_id_verdict_idx
    ON blast_radius_verdicts (analysis_id, reachable_verdict);

-- Performance monitoring — one row per (analysis, stage), independent of the
-- ecosystem-specific pipeline tables above so it keeps working unchanged as
-- more ecosystems (and possibly different stage sets) come online.
CREATE TABLE IF NOT EXISTS blast_radius_stage_runs (
    id              BIGSERIAL PRIMARY KEY,
    analysis_id     UUID NOT NULL REFERENCES blast_radius_analyses (id),
    stage           TEXT NOT NULL CHECK (stage IN ('intel', 'dependents', 'reachability', 'report')),
    status          TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'succeeded', 'failed')),
    model           TEXT, -- agent model used by this stage, NULL for deterministic stages
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    duration_ms     BIGINT,
    cost_usd        NUMERIC(10, 4) NOT NULL DEFAULT 0,
    error           TEXT,
    UNIQUE (analysis_id, stage)
);

CREATE INDEX IF NOT EXISTS blast_radius_stage_runs_analysis_id_idx
    ON blast_radius_stage_runs (analysis_id);
