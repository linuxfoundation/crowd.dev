-- Wire packages-db into the Sequin → Kafka → Tinybird pipeline.
--
-- Two related changes bundled here because both serve the same goal — making
-- packages-db row changes replicate cleanly into Tinybird:
--
--   1. Publication + REPLICA IDENTITY FULL on the 11 tables the Tinybird
--      datasources read from. publish_via_partition_root collapses the
--      versions (32) / package_dependencies (64) partition leaves into a
--      single logical topic each. REPLICA IDENTITY on a partitioned root
--      does not cascade, so every leaf is set explicitly via pg_inherits.
--
--   2. rank_packages() bumps last_synced_at on every UPDATE that touches a
--      DS-exported field (impact, is_critical, last_rank_pass_at).
--      last_synced_at is the Tinybird ENGINE_VER for the packages datasource;
--      without this bump, ReplacingMergeTree may keep an older row when
--      criticality changes without any other write path touching the row.

-- ─── 1. Sequin publication ──────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'sequin_pub'
    ) THEN
        CREATE PUBLICATION sequin_pub
            FOR TABLE
                packages,
                versions,
                package_dependencies,
                package_maintainers,
                package_repos,
                maintainers,
                repos,
                repo_scorecard_checks,
                advisories,
                advisory_packages,
                advisory_affected_ranges
            WITH (publish_via_partition_root = true);
    END IF;
END$$;

ALTER TABLE public.packages                 REPLICA IDENTITY FULL;
ALTER TABLE public.versions                 REPLICA IDENTITY FULL;
ALTER TABLE public.package_dependencies     REPLICA IDENTITY FULL;
ALTER TABLE public.package_maintainers      REPLICA IDENTITY FULL;
ALTER TABLE public.package_repos            REPLICA IDENTITY FULL;
ALTER TABLE public.maintainers              REPLICA IDENTITY FULL;
ALTER TABLE public.repos                    REPLICA IDENTITY FULL;
ALTER TABLE public.repo_scorecard_checks    REPLICA IDENTITY FULL;
ALTER TABLE public.advisories               REPLICA IDENTITY FULL;
ALTER TABLE public.advisory_packages        REPLICA IDENTITY FULL;
ALTER TABLE public.advisory_affected_ranges REPLICA IDENTITY FULL;

-- versions (32) and package_dependencies (64) are hash-partitioned. REPLICA
-- IDENTITY on the partitioned root does not cascade; set it on every leaf.
DO $$
DECLARE
    parent_table  text;
    partition_oid regclass;
BEGIN
    FOREACH parent_table IN ARRAY ARRAY['public.versions', 'public.package_dependencies']
    LOOP
        FOR partition_oid IN
            SELECT inhrelid::regclass
            FROM pg_inherits
            WHERE inhparent = parent_table::regclass
        LOOP
            EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', partition_oid);
        END LOOP;
    END LOOP;
END$$;

-- ─── 2. rank_packages() bumps last_synced_at ────────────────────────────────

CREATE OR REPLACE FUNCTION rank_packages(
    weight_downloads            numeric DEFAULT 0.25,
    weight_dependent_packages   numeric DEFAULT 0.25,
    weight_transitive           numeric DEFAULT 0.50,
    critical_top_n_by_ecosystem jsonb   DEFAULT '{"npm":400000,"go":100000,"maven":200000,"pypi":100000,"nuget":50000,"cargo":75000}'::jsonb
)
RETURNS TABLE(scored_rows int, ranked_rows int)
LANGUAGE plpgsql AS $$
DECLARE
    n_scored int;
    n_ranked int;
BEGIN
    -- Step 1: score
    WITH percentile_scores AS (
        SELECT
            id,
            (
              weight_downloads * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LOG(1 + COALESCE(downloads_last_30d, 0)))

            + weight_dependent_packages * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LOG(1 + COALESCE(dependent_count, 0)))

            + weight_transitive * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LOG(1 + COALESCE(transitive_dependent_count, 0)))
            )::numeric(10, 4) AS new_impact
        FROM packages
        WHERE ecosystem IN (SELECT jsonb_object_keys(critical_top_n_by_ecosystem))
    )
    UPDATE packages p
       SET impact         = ps.new_impact,
           last_synced_at = NOW()
      FROM percentile_scores ps
     WHERE p.id = ps.id
       AND p.impact IS DISTINCT FROM ps.new_impact;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    -- Step 2: rank + flag
    WITH ranked AS (
        SELECT
            id, ecosystem,
            ROW_NUMBER() OVER (
                PARTITION BY ecosystem
                ORDER BY impact DESC NULLS LAST, id
            ) AS r
        FROM packages
        WHERE purl IS NOT NULL
          AND ecosystem IN (SELECT jsonb_object_keys(critical_top_n_by_ecosystem))
    ),
    flagged AS (
        SELECT
            id, r,
            COALESCE(
                r <= (critical_top_n_by_ecosystem ->> ecosystem)::int,
                FALSE
            ) AS new_is_critical
        FROM ranked
    )
    UPDATE packages p
       SET rank_in_ecosystem = f.r,
           is_critical       = f.new_is_critical,
           last_synced_at    = NOW()
      FROM flagged f
     WHERE p.id = f.id
       AND (
             p.rank_in_ecosystem IS DISTINCT FROM f.r
          OR p.is_critical       IS DISTINCT FROM f.new_is_critical
       );

    GET DIAGNOSTICS n_ranked = ROW_COUNT;

    -- Step 2.5: spotlight overrides
    UPDATE packages p
       SET is_critical    = TRUE,
           last_synced_at = NOW()
      FROM package_criticality_spotlight s
     WHERE p.ecosystem                    = s.ecosystem
       AND (p.namespace IS NOT DISTINCT FROM s.namespace)
       AND p.name                         = s.name
       AND p.is_critical                  = FALSE;

    -- Step 3: stamp last_rank_pass_at unconditionally
    UPDATE packages
       SET last_rank_pass_at = NOW(),
           last_synced_at    = NOW()
     WHERE ecosystem IN (SELECT jsonb_object_keys(critical_top_n_by_ecosystem));

    RETURN QUERY SELECT n_scored, n_ranked;
END;
$$;
