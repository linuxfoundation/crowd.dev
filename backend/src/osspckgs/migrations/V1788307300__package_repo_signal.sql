-- Secondary manifest repository signal (CM-1393): a declared link sourced from a
-- fallback field (homepage/bug_tracker) scores one tier below a dedicated repository field.

ALTER TABLE package_repos
    ADD COLUMN IF NOT EXISTS signal text NOT NULL DEFAULT 'primary';

-- NOT VALID + separate VALIDATE so the existing-row scan takes the less disruptive
-- validation lock instead of blocking registry writers under ADD COLUMN's stronger lock.
ALTER TABLE package_repos
    ADD CONSTRAINT package_repos_signal_check CHECK (signal IN ('primary', 'secondary')) NOT VALID;
ALTER TABLE package_repos
    VALIDATE CONSTRAINT package_repos_signal_check;

CREATE OR REPLACE FUNCTION package_repo_confidence(
    p_source           text,
    p_ecosystem        text,
    p_signal           text,
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

    -- Signal adjusts the declared tier only. A deps.dev publish attestation already
    -- proves the publisher–repo relationship, and manual links are operator-pinned.
    IF p_source = 'declared' AND p_signal = 'secondary' THEN
        base := base - 0.10;
    END IF;

    source_priority := CASE p_source
        WHEN 'manual'    THEN 3
        WHEN 'deps_dev'  THEN 2
        WHEN 'declared'  THEN 1
        ELSE 0
    END;

    IF p_disabled IS TRUE THEN
        -- Scale proportionally to preserve source ordering; the tighter modulo keeps the max
        -- offset contribution (≈4e-6) below the 0.00016 minimum tier gap so it can't invert it.
        base := 0.05 + LEAST(base, 0.99) * 0.004;
        offset_units := source_priority::bigint * 1000 + COALESCE(p_repo_id, 0) % 1000;
    ELSE
        IF p_archived IS TRUE THEN
            base := base - 0.20;
        END IF;

        IF p_is_fork IS TRUE THEN
            base := base - 0.10;
        END IF;

        IF p_competing_github IS TRUE AND p_host IS NOT NULL AND p_host <> 'github' THEN
            base := base - 0.05;
        END IF;

        base := GREATEST(base, 0.05);

        -- Tie-breaker: collisions only when two repo IDs for the same package are congruent
        -- mod 1,000,000; BEST_REPO_LINK_JOIN's ORDER BY repo_id DESC then picks deterministically.
        offset_units := source_priority::bigint * 1000000 + COALESCE(p_repo_id, 0) % 1000000;
    END IF;

    RETURN LEAST(base + offset_units * 0.000000001, 0.999999999);
END;
$$;

-- Compat overload for callers still on the pre-signal signature during a rolling deploy;
-- delegates to the widened function as 'primary'. Drop in a later cleanup migration.
CREATE OR REPLACE FUNCTION package_repo_confidence(
    p_source           text,
    p_ecosystem        text,
    p_provenance       text,
    p_archived         bool,
    p_is_fork          bool,
    p_disabled         bool,
    p_host             text,
    p_competing_github bool,
    p_repo_id          bigint
)
RETURNS numeric(12, 9)
LANGUAGE sql IMMUTABLE AS $$
    SELECT package_repo_confidence(
        p_source, p_ecosystem, 'primary', p_provenance, p_archived,
        p_is_fork, p_disabled, p_host, p_competing_github, p_repo_id
    )
$$;

-- Replaced only to pass cur.signal through to the widened scoring function; the
-- chunking, locking and keyset paging are unchanged from V1788307200.
CREATE OR REPLACE PROCEDURE rescore_package_repo_confidence(
    p_repo_ids   bigint[] DEFAULT NULL,
    chunk_size   int      DEFAULT 25000,
    INOUT applied_rows int DEFAULT 0
)
LANGUAGE plpgsql AS $$
DECLARE
    batch_rows   int;
    updated_rows int;
    cursor_id    bigint := 0;
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
                   -- deps_dev rows with NULL provenance predate this column; skip them so the
                   -- backfill doesn't downgrade SLSA/attestation links — a later ingest fixes it.
                   AND NOT (pr.source = 'deps_dev' AND pr.provenance IS NULL)
                 ORDER BY pr.id
                 LIMIT chunk_size
                   FOR UPDATE
            ),
            updated AS (
                UPDATE package_repos pr
                   SET confidence = s.confidence, verified_at = NOW()
                  FROM batch b
                  JOIN package_repos cur ON cur.id = b.id
                  JOIN packages p ON p.id = cur.package_id
                  JOIN repos r ON r.id = cur.repo_id,
                       LATERAL (
                         SELECT package_repo_confidence(
                           cur.source, p.ecosystem, cur.signal, cur.provenance,
                           r.archived, r.is_fork, r.disabled, r.host,
                           EXISTS (
                             SELECT 1
                               FROM package_repos c
                               JOIN repos cr ON cr.id = c.repo_id
                              WHERE c.package_id = cur.package_id
                                AND c.repo_id <> cur.repo_id
                                AND cr.host = 'github'
                           ),
                           cur.repo_id
                         ) AS confidence
                       ) s
                 WHERE pr.id = b.id
                   AND s.confidence IS DISTINCT FROM cur.confidence
                RETURNING pr.id
            )
            SELECT COUNT(*), COALESCE(MAX(b.id), cursor_id),
                   (SELECT COUNT(*) FROM updated)
              INTO batch_rows, cursor_id, updated_rows
              FROM batch b;

            applied_rows := applied_rows + updated_rows;

            COMMIT;

            EXIT WHEN batch_rows < chunk_size;
        END LOOP;

    PERFORM pg_advisory_unlock(hashtextextended('rescore_package_repo_confidence', 0));
END;
$$;
