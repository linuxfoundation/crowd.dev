-- Deterministic package→repo confidence scoring (CM-1306).
--
-- confidence numeric(3,2) cannot hold the uniqueness offset that makes
-- ORDER BY confidence DESC LIMIT 1 unambiguous, so it is widened to numeric(12,9).
-- Scale changes rewrite the table under an ACCESS EXCLUSIVE lock, and package_repos
-- carries REPLICA IDENTITY FULL plus a Sequin publication (see V1781009234) — run this
-- with the Sequin and Tinybird sinks paused.
--
-- Existing rows keep their current value until rescore_package_repo_confidence()
-- backfills them in chunks; every new write goes through the scoring function below.

ALTER TABLE package_repos
    ALTER COLUMN confidence TYPE numeric(12, 9),
    -- Which manifest field produced the link. 'secondary' is written by CM-1393.
    ADD COLUMN IF NOT EXISTS signal text NOT NULL DEFAULT 'primary'
        CHECK (signal IN ('primary', 'secondary')),
    -- Ownership evidence for a declared link. Real values written by CM-1394;
    -- 'no_evidence' is the rollout default — no penalty applied until CM-1394 sets real values.
    ADD COLUMN IF NOT EXISTS ownership_match text NOT NULL DEFAULT 'no_evidence'
        CHECK (ownership_match IN ('matched', 'unmatched', 'no_evidence')),
    -- deps.dev RelationProvenance for source='deps_dev'. Previously collapsed to a
    -- confidence number inside the BigQuery query, which left nothing to rescore from.
    ADD COLUMN IF NOT EXISTS provenance text;

-- Serves the pick-highest lateral in BEST_REPO_LINK_JOIN.
CREATE INDEX IF NOT EXISTS package_repos_package_id_confidence_idx
    ON package_repos (package_id, confidence DESC);

-- ============================================================
-- Scoring function — the only path that may produce a confidence value.
-- ============================================================
--
-- Base tier by source, then repo-state penalties, then a deterministic offset that
-- breaks ties. Every scale here is intentional: base tiers sit on 0.05 boundaries,
-- penalties on 0.05 boundaries, and the offset is bounded below 0.004 so it can
-- never cross a tier boundary or a High/Medium/Low threshold (0.80 / 0.50).
CREATE OR REPLACE FUNCTION package_repo_confidence(
    p_source           text,
    p_ecosystem        text,
    p_signal           text,
    p_ownership_match  text,
    p_provenance       text,
    p_archived         bool,
    p_is_fork          bool,
    p_disabled         bool,
    p_host             text,
    p_competing_github bool,
    p_repo_id          bigint
)
RETURNS numeric(12, 9)
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    base            numeric;
    source_priority int;
    offset_units    bigint;
BEGIN
    base := CASE p_source
        WHEN 'manual'    THEN 0.99
        WHEN 'heuristic' THEN 0.30
        WHEN 'deps_dev'  THEN CASE p_provenance
            WHEN 'SLSA_ATTESTATION'             THEN 0.99
            WHEN 'RUBYGEMS_PUBLISH_ATTESTATION' THEN 0.95
            WHEN 'PYPI_PUBLISH_ATTESTATION'     THEN 0.95
            WHEN 'GO_ORIGIN'                    THEN 0.90
            ELSE 0.50
        END
        -- maven splits off npm/cargo/the rest: POM <scm> blocks are notoriously
        -- stale (legacy SVN URLs, org renames, dead mirrors).
        WHEN 'declared' THEN CASE WHEN p_ecosystem = 'maven' THEN 0.80 ELSE 0.85 END
        ELSE 0.30
    END;

    -- Signal and ownership adjust the declared tier only. A deps.dev publish
    -- attestation already proves the publisher–repo relationship, and manual links
    -- are operator-pinned.
    IF p_source = 'declared' THEN
        IF p_signal = 'secondary' THEN
            base := base - 0.10;
        END IF;

        IF p_ownership_match = 'unmatched' THEN
            base := base - 0.25;
        END IF;
    END IF;

    -- Disabled overrides all state penalties but still gets the uniqueness offset so
    -- the no-ties invariant holds and a stronger claim can displace the stored row.
    IF p_disabled IS TRUE THEN
        base := 0.05;
    ELSE
        IF p_archived IS TRUE THEN
            base := base - 0.20;
        END IF;

        IF p_is_fork IS TRUE THEN
            base := base - 0.10;
        END IF;

        IF p_competing_github IS TRUE AND COALESCE(p_host, '') <> 'github' THEN
            base := base - 0.05;
        END IF;
    END IF;

    base := GREATEST(base, 0.05);

    source_priority := CASE p_source
        WHEN 'manual'    THEN 3
        WHEN 'deps_dev'  THEN 2
        WHEN 'declared'  THEN 1
        ELSE 0
    END;

    -- Two repos on the same package collide only if their ids are congruent mod 1e6
    -- and their sources share a priority band.
    offset_units := source_priority::bigint * 1000000 + COALESCE(p_repo_id, 0) % 1000000;

    RETURN LEAST(base + offset_units * 0.000000001, 0.999999999);
END;
$$;

-- ============================================================
-- Rescore — chunked, keyset-paged, committed per chunk so the Sequin slot advances.
-- ============================================================
--
-- Used for the initial backfill (p_repo_ids NULL = whole table) and as the daily
-- safety-net sweep. The enricher rescores its own touched repos inline.
CREATE OR REPLACE PROCEDURE rescore_package_repo_confidence(
    p_repo_ids   bigint[] DEFAULT NULL,
    chunk_size   int      DEFAULT 25000,
    INOUT applied_rows int DEFAULT 0
)
LANGUAGE plpgsql AS $$
DECLARE
    batch_rows int;
    cursor_id  bigint := 0;
BEGIN
    IF chunk_size IS NULL OR chunk_size <= 0 THEN
        RAISE EXCEPTION 'rescore_package_repo_confidence: chunk_size must be positive, got %', chunk_size;
    END IF;

    -- Session-level: survives the internal COMMITs below.
    IF NOT pg_try_advisory_lock(hashtextextended('rescore_package_repo_confidence', 0)) THEN
        RAISE EXCEPTION 'rescore_package_repo_confidence: another execution is already in progress';
    END IF;

    applied_rows := 0;

    LOOP
        WITH batch AS (
            SELECT pr.id
              FROM package_repos pr
             WHERE pr.id > cursor_id
               AND (p_repo_ids IS NULL OR pr.repo_id = ANY(p_repo_ids))
             ORDER BY pr.id
             LIMIT chunk_size
        ),
        updated AS (
            UPDATE package_repos pr
               SET confidence = s.confidence
              FROM batch b, packages p, repos r,
                   LATERAL (
                     SELECT package_repo_confidence(
                       pr.source, p.ecosystem, pr.signal, pr.ownership_match, pr.provenance,
                       r.archived, r.is_fork, r.disabled, r.host,
                       EXISTS (
                         SELECT 1
                           FROM package_repos c
                           JOIN repos cr ON cr.id = c.repo_id
                          WHERE c.package_id = pr.package_id
                            AND c.repo_id <> pr.repo_id
                            AND cr.host = 'github'
                       ),
                       pr.repo_id
                     ) AS confidence
                   ) s
             WHERE pr.id = b.id
               AND p.id = pr.package_id
               AND r.id = pr.repo_id
            RETURNING pr.id
        )
        SELECT COUNT(*), COALESCE(MAX(b.id), cursor_id)
          INTO batch_rows, cursor_id
          FROM batch b;

        applied_rows := applied_rows + batch_rows;

        COMMIT;

        EXIT WHEN batch_rows < chunk_size;
    END LOOP;

    PERFORM pg_advisory_unlock(hashtextextended('rescore_package_repo_confidence', 0));
END;
$$;
