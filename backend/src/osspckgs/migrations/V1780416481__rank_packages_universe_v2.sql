-- Replaces the placeholder rank_packages_universe() with the full ADR formula.
--
-- Formula per ADR-0001 Criticality scoring methodology:
--   score = w_centrality  * pct_rank( centrality_score                   ) within ecosystem
--         + w_transitive  * pct_rank( LN(1 + transitive_dependent_count) ) within ecosystem
--         + w_dep_pkgs    * pct_rank( LN(1 + dependent_packages_count)   ) within ecosystem
--         + w_dep_repos   * pct_rank( LN(1 + dependent_repos_count)      ) within ecosystem
--         + w_downloads   * pct_rank( LN(1 + downloads_30d)              ) within ecosystem
--
-- All five weights must sum to 1.0 — caller's responsibility.
-- centrality_score skips LN(): PageRank is already bounded in (0,1].
-- COALESCE(x, 0) gives missing signals the floor percentile rather than an error.
-- ROW_NUMBER() (not RANK()) keeps each ecosystem's critical set exactly at top-N.

CREATE OR REPLACE FUNCTION rank_packages_universe(
    weight_centrality           numeric,
    weight_transitive           numeric,
    weight_dependent_packages   numeric,
    weight_dependent_repos      numeric,
    weight_downloads            numeric,
    critical_top_n_by_ecosystem jsonb
)
RETURNS TABLE(scored_rows int, ranked_rows int, propagated_rows int)
LANGUAGE plpgsql AS $$
DECLARE
    n_scored     int;
    n_ranked     int;
    n_propagated int;
BEGIN
    -- ── Step 1: score ──────────────────────────────────────────────────────────
    WITH percentile_scores AS (
        SELECT
            id,
            (
              weight_centrality * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY COALESCE(centrality_score, 0))

            + weight_transitive * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LN(1 + COALESCE(transitive_dependent_count, 0)))

            + weight_dependent_packages * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LN(1 + COALESCE(dependent_packages_count, 0)))

            + weight_dependent_repos * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LN(1 + COALESCE(dependent_repos_count, 0)))

            + weight_downloads * PERCENT_RANK() OVER (
                  PARTITION BY ecosystem ORDER BY LN(1 + COALESCE(downloads_30d, 0)))
            )::numeric(10, 4) AS new_score
        FROM packages_universe
    )
    UPDATE packages_universe pu
       SET criticality_score = ps.new_score,
           last_rank_pass_at = NOW()
      FROM percentile_scores ps
     WHERE pu.id = ps.id
       AND pu.criticality_score IS DISTINCT FROM ps.new_score;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    -- ── Step 2: rank + flag ────────────────────────────────────────────────────
    WITH ranked AS (
        SELECT
            id, ecosystem,
            ROW_NUMBER() OVER (
                PARTITION BY ecosystem
                ORDER BY criticality_score DESC NULLS LAST, id
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

    -- ── Step 3: propagate to packages ─────────────────────────────────────────
    UPDATE packages p
       SET criticality_score = pu.criticality_score,
           is_critical       = pu.is_critical,
           last_rank_pass_at = NOW()
      FROM packages_universe pu
     WHERE p.purl      = pu.purl
       AND p.ecosystem = pu.ecosystem
       AND (
             p.criticality_score IS DISTINCT FROM pu.criticality_score
          OR p.is_critical       IS DISTINCT FROM pu.is_critical
       );

    GET DIAGNOSTICS n_propagated = ROW_COUNT;

    RETURN QUERY SELECT n_scored, n_ranked, n_propagated;
END;
$$;
