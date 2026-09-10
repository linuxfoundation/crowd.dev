-- rank_packages() applied its ranking to all of `packages` in one UPDATE (9.66M
-- rows), which under REPLICA IDENTITY FULL blew up the Sequin replication slot.
-- rank_packages_chunked() stages the same ranking into an UNLOGGED table, then
-- applies it in committed keyset chunks so the slot advances continuously.
-- rank_packages() is left in place until the new worker deploys.

CREATE UNLOGGED TABLE IF NOT EXISTS staging.package_rank (
    package_id        bigint PRIMARY KEY,
    impact            numeric(10, 4),
    is_critical       bool NOT NULL,
    rank_in_ecosystem int  NOT NULL
);

CREATE OR REPLACE PROCEDURE rank_packages_chunked(
    coverage_cutoff numeric DEFAULT 0.90,
    ecosystems      text[]  DEFAULT NULL,
    chunk_size      int     DEFAULT 25000,
    INOUT applied_rows int  DEFAULT 0
)
LANGUAGE plpgsql AS $$
DECLARE
    effective_ecosystems text[];
    staged_count          int;
    batch_rows            int;
    cursor_id             bigint := 0;
BEGIN
    SET LOCAL max_parallel_workers_per_gather = 4;

    IF chunk_size IS NULL OR chunk_size <= 0 THEN
        RAISE EXCEPTION 'rank_packages_chunked: chunk_size must be a positive integer, got %', chunk_size;
    END IF;

    -- Session-level: survives the internal COMMITs below
    IF NOT pg_try_advisory_lock(hashtextextended('rank_packages_chunked', 0)) THEN
        RAISE EXCEPTION 'rank_packages_chunked: another execution is already in progress';
    END IF;

    applied_rows := 0;

    IF ecosystems IS NULL THEN
        SELECT ARRAY_AGG(DISTINCT ecosystem)
          INTO effective_ecosystems
          FROM packages;
    ELSE
        effective_ecosystems := ecosystems;
    END IF;

    TRUNCATE staging.package_rank;

    -- Scoring CTE chain, unchanged from rank_packages() (V1783123201).
    INSERT INTO staging.package_rank (package_id, impact, is_critical, rank_in_ecosystem)
    WITH base AS (
        SELECT
            id,
            ecosystem,
            COALESCE(downloads_last_30d,         0) AS downloads,
            COALESCE(dependent_count,            0) AS direct_dependents,
            COALESCE(transitive_dependent_count, 0) AS transitive_dependents,
            COALESCE(sonatype_popularity_score,  0) AS sonatype_popularity,
            SUM(COALESCE(downloads_last_30d,         0)) OVER (PARTITION BY ecosystem) AS ecosystem_total_downloads,
            SUM(COALESCE(dependent_count,            0)) OVER (PARTITION BY ecosystem) AS ecosystem_total_direct_dependents,
            SUM(COALESCE(transitive_dependent_count, 0)) OVER (PARTITION BY ecosystem) AS ecosystem_total_transitive_dependents,
            SUM(COALESCE(sonatype_popularity_score,  0)) OVER (PARTITION BY ecosystem) AS ecosystem_total_sonatype
        FROM packages
        WHERE ecosystem = ANY(effective_ecosystems)
    ),
    walked AS (
        SELECT
            id,
            ecosystem,
            SUM(signal_value) OVER coverage_window / ecosystem_signal_total::numeric                  AS cumulative_share_inclusive,
            (SUM(signal_value) OVER coverage_window - signal_value) / ecosystem_signal_total::numeric AS cumulative_share_exclusive
        FROM base
        CROSS JOIN LATERAL (VALUES
            ('downloads',             downloads,             ecosystem_total_downloads),
            ('direct_dependents',     direct_dependents,     ecosystem_total_direct_dependents),
            ('transitive_dependents', transitive_dependents, ecosystem_total_transitive_dependents),
            ('sonatype_popularity',   sonatype_popularity,   ecosystem_total_sonatype)
        ) AS signal(signal_name, signal_value, ecosystem_signal_total)
        WHERE ecosystem_signal_total > 0
        WINDOW coverage_window AS (
            PARTITION BY ecosystem, signal_name
            ORDER BY signal_value DESC, id
            ROWS UNBOUNDED PRECEDING
        )
    ),
    combined AS (
        SELECT
            id,
            ecosystem,
            AVG(1.0 - cumulative_share_inclusive)::numeric(10, 4) AS new_impact,
            BOOL_OR(cumulative_share_exclusive < coverage_cutoff)  AS new_is_critical
        FROM walked
        GROUP BY id, ecosystem
    ),
    final AS (
        SELECT
            combined.id,
            combined.new_impact,
            combined.new_is_critical OR (spotlight.package_id IS NOT NULL) AS new_is_critical,
            ROW_NUMBER() OVER (
                PARTITION BY combined.ecosystem
                ORDER BY combined.new_impact DESC NULLS LAST, combined.id
            ) AS new_rank_in_ecosystem
        FROM combined
        LEFT JOIN package_criticality_spotlight spotlight ON spotlight.package_id = combined.id
    )
    SELECT id, new_impact, new_is_critical, new_rank_in_ecosystem::int
      FROM final;

    GET DIAGNOSTICS staged_count = ROW_COUNT;

    IF staged_count = 0 THEN
        RAISE EXCEPTION 'rank_packages_chunked: computed 0 rows, refusing to apply an empty ranking';
    END IF;

    ANALYZE staging.package_rank;

    COMMIT;

    LOOP
        WITH batch AS (
            SELECT package_id, impact, is_critical, rank_in_ecosystem
              FROM staging.package_rank
             WHERE package_id > cursor_id
             ORDER BY package_id
             LIMIT chunk_size
        ),
        updated AS (
            UPDATE packages p
               SET impact            = b.impact,
                   is_critical       = b.is_critical,
                   rank_in_ecosystem = b.rank_in_ecosystem,
                   last_rank_pass_at = NOW(),
                   last_synced_at    = NOW()
              FROM batch b
             WHERE p.id = b.package_id
            RETURNING p.id
        )
        SELECT COUNT(*), COALESCE(MAX(b.package_id), cursor_id)
          INTO batch_rows, cursor_id
          FROM batch b;

        applied_rows := applied_rows + batch_rows;

        COMMIT;

        EXIT WHEN batch_rows < chunk_size;
    END LOOP;

    PERFORM pg_advisory_unlock(hashtextextended('rank_packages_chunked', 0));
END;
$$;
