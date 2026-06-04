-- Renames criticality_score → impact on both packages_universe and packages,
-- and installs rank_packages_universe() with the updated formula.
--
-- Formula (ADR-0001 §Criticality scoring methodology):
--   impact = w_downloads  * pct_rank( LOG(1 + downloads_last_30d)          ) within ecosystem
--           + w_dep_pkgs  * pct_rank( LOG(1 + dependent_count)             ) within ecosystem
--           + w_transitive * pct_rank( LOG(1 + transitive_dependent_count) ) within ecosystem
--
-- Default weights: 0.25 / 0.25 / 0.50 (sum to 1.0).
-- All weights and the top-N budget are call-time parameters — tunable without
-- schema or code changes.
--
-- Steps inside the function:
--   1. Score    — compute impact via weighted PERCENT_RANK()
--   2. Rank     — ROW_NUMBER() per ecosystem, flag top-N as is_critical
--   2.5 Spotlight — force is_critical = TRUE for rows in package_criticality_spotlight
--   3. Propagate — copy impact + is_critical onto the packages table

ALTER TABLE packages_universe
    RENAME COLUMN criticality_score TO impact;

ALTER TABLE packages
    RENAME COLUMN criticality_score TO impact;

CREATE OR REPLACE FUNCTION rank_packages_universe(
    weight_downloads            numeric DEFAULT 0.25,
    weight_dependent_packages   numeric DEFAULT 0.25,
    weight_transitive           numeric DEFAULT 0.50,
    critical_top_n_by_ecosystem jsonb   DEFAULT '{}'::jsonb
)
RETURNS TABLE(scored_rows int, ranked_rows int, propagated_rows int)
LANGUAGE plpgsql AS $$
DECLARE
    n_scored     int;
    n_ranked     int;
    n_propagated int;
BEGIN
    -- ── Step 1: score ──────────────────────────────────────────────────────────
    -- last_rank_pass_at updated unconditionally on every pass (schema requirement).
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
        FROM packages_universe
    )
    UPDATE packages_universe pu
       SET impact            = ps.new_impact,
           last_rank_pass_at = NOW()
      FROM percentile_scores ps
     WHERE pu.id = ps.id;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    -- ── Step 2: rank + flag ────────────────────────────────────────────────────
    WITH ranked AS (
        SELECT
            id, ecosystem,
            ROW_NUMBER() OVER (
                PARTITION BY ecosystem
                ORDER BY impact DESC NULLS LAST, id
            ) AS r
        FROM packages_universe
        WHERE purl IS NOT NULL
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
    UPDATE packages_universe pu
       SET rank_in_ecosystem = f.r,
           is_critical       = f.new_is_critical
      FROM flagged f
     WHERE pu.id = f.id
       AND (
             pu.rank_in_ecosystem IS DISTINCT FROM f.r
          OR pu.is_critical       IS DISTINCT FROM f.new_is_critical
       );

    GET DIAGNOSTICS n_ranked = ROW_COUNT;

    -- ── Step 2.5: apply spotlight overrides ───────────────────────────────────
    -- Force is_critical = TRUE for any row in package_criticality_spotlight,
    -- regardless of computed score or rank. Runs after Step 2 so overrides
    -- survive every automated re-rank pass.
    -- IS NOT DISTINCT FROM handles the NULL namespace case (e.g. cargo crates).
    UPDATE packages_universe pu
       SET is_critical = TRUE
      FROM package_criticality_spotlight s
     WHERE pu.ecosystem                    = s.ecosystem
       AND (pu.namespace IS NOT DISTINCT FROM s.namespace)
       AND pu.name                         = s.name
       AND pu.is_critical                  = FALSE;

    -- ── Step 3: propagate to packages ─────────────────────────────────────────
    -- last_rank_pass_at updated unconditionally on every pass (schema requirement).
    UPDATE packages p
       SET impact            = pu.impact,
           is_critical       = pu.is_critical,
           last_rank_pass_at = NOW()
      FROM packages_universe pu
     WHERE p.purl      = pu.purl
       AND p.ecosystem = pu.ecosystem;

    GET DIAGNOSTICS n_propagated = ROW_COUNT;

    RETURN QUERY SELECT n_scored, n_ranked, n_propagated;
END;
$$;
