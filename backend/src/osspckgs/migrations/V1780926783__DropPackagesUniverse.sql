-- Retire packages_universe (Tier 3 workspace).  All signals are migrated onto
-- packages, rank_packages_universe() is replaced by rank_packages() which operates on packages directly,
-- and the table is dropped.
--
-- Columns migrated from packages_universe → packages:
--   downloads_last_30d   bigint         (npm 30-day window cache for ranking)
--   centrality_score     numeric(10,8)  (PageRank, stored for future formula use)
--   rank_in_ecosystem    int            (computed by rank_packages)
--
-- rank_packages_universe() → rank_packages() changes:
--   - Operates on packages directly; no more TRUNCATE/INSERT workspace.
--   - Scope limited to ecosystems present in critical_top_n_by_ecosystem JSONB
--     (dynamic — add an ecosystem to the JSONB to include it in ranking).
--   - "Propagate to packages" step removed (packages IS the target now).
--   - Return column propagated_rows removed (no propagation step).

-- ── 1. Add missing columns to packages ────────────────────────────────────────

ALTER TABLE packages
  ADD COLUMN IF NOT EXISTS downloads_last_30d  bigint,
  ADD COLUMN IF NOT EXISTS centrality_score    numeric(10, 8),
  ADD COLUMN IF NOT EXISTS rank_in_ecosystem   int;

-- ── 2. Back-fill from packages_universe ───────────────────────────────────────

UPDATE packages p
   SET downloads_last_30d = pu.downloads_last_30d,
       centrality_score   = pu.centrality_score,
       rank_in_ecosystem  = pu.rank_in_ecosystem
  FROM packages_universe pu
 WHERE p.purl = pu.purl;

-- ── 3. Replace rank_packages_universe() ───────────────────────────────────────
-- Two overloads exist in the schema:
--   V1779710880 created (numeric, numeric, numeric, numeric, jsonb) — 5 params
--   V1780589607 CREATE OR REPLACE'd with (numeric, numeric, numeric, jsonb) — different
--     signature, so it added a second overload rather than replacing the first.
-- Both must be dropped; only one new 4-param version is created.

DROP FUNCTION IF EXISTS rank_packages_universe(numeric, numeric, numeric, numeric, jsonb);
DROP FUNCTION IF EXISTS rank_packages_universe(numeric, numeric, numeric, jsonb);

-- Usage:
--   -- with defaults (weights 0.25/0.25/0.50, built-in top-N budget)
--   SELECT * FROM rank_packages();
--
--   -- with custom weights and/or a different top-N budget
--   SELECT * FROM rank_packages(
--     0.20, 0.30, 0.50,
--     '{"npm": 400000, "maven": 200000, "cargo": 75000}'::jsonb
--   );

-- rank_packages() — score, rank, and flag packages in one pass.
--
-- Formula:
--   impact = w_downloads  * pct_rank( LOG(1 + downloads_last_30d)              ) within ecosystem
--           + w_dep_pkgs  * pct_rank( LOG(1 + dependent_count)            ) within ecosystem
--           + w_transitive * pct_rank( LOG(1 + transitive_dependent_count) ) within ecosystem
--
-- Steps:
--   1. Score    — compute impact via weighted PERCENT_RANK() (scoped to JSONB ecosystems)
--   2. Rank     — ROW_NUMBER() per ecosystem, flag top-N as is_critical (scoped to JSONB ecosystems)
--   2.5 Spotlight — force is_critical = TRUE for rows in package_criticality_spotlight
--   3. Stamp    — unconditionally set last_rank_pass_at on all scored rows (schema contract)
--
-- All weights and the top-N budget are call-time parameters.
-- ROW_NUMBER() (not RANK()) keeps each ecosystem's critical set exactly at top-N.
-- Only ecosystems present as keys in critical_top_n_by_ecosystem are scored/ranked;
-- packages from other ecosystems are not touched.

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
    -- ── Step 1: score ──────────────────────────────────────────────────────────
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
       SET impact = ps.new_impact
      FROM percentile_scores ps
     WHERE p.id = ps.id
       AND p.impact IS DISTINCT FROM ps.new_impact;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    -- ── Step 2: rank + flag ────────────────────────────────────────────────────
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
           is_critical       = f.new_is_critical
      FROM flagged f
     WHERE p.id = f.id
       AND (
             p.rank_in_ecosystem IS DISTINCT FROM f.r
          OR p.is_critical       IS DISTINCT FROM f.new_is_critical
       );

    GET DIAGNOSTICS n_ranked = ROW_COUNT;

    -- ── Step 2.5: spotlight overrides ─────────────────────────────────────────
    UPDATE packages p
       SET is_critical = TRUE
      FROM package_criticality_spotlight s
     WHERE p.ecosystem                    = s.ecosystem
       AND (p.namespace IS NOT DISTINCT FROM s.namespace)
       AND p.name                         = s.name
       AND p.is_critical                  = FALSE;

    -- ── Step 3: stamp last_rank_pass_at unconditionally ───────────────────────
    -- Schema contract: must be updated on every pass (not only when scores change)
    -- so stale-detection queries (last_rank_pass_at < NOW() - INTERVAL '8 days') work.
    UPDATE packages
       SET last_rank_pass_at = NOW()
     WHERE ecosystem IN (SELECT jsonb_object_keys(critical_top_n_by_ecosystem));

    RETURN QUERY SELECT n_scored, n_ranked;
END;
$$;

-- ── 4. Drop packages_universe ─────────────────────────────────────────────────
-- No FK constraints reference this table (npm_package_universe_state and
-- downloads_last_30d use purl text, not FK).

DROP TABLE packages_universe;
