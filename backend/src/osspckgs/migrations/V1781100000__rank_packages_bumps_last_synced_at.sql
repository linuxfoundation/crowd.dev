-- rank_packages() now bumps last_synced_at on every UPDATE that touches a
-- DS-exported field (impact, is_critical, last_rank_pass_at). last_synced_at
-- is the Tinybird ENGINE_VER for the packages datasource; without this bump,
-- ReplacingMergeTree may keep an older row when criticality changes without
-- any other write path touching the package row.

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
