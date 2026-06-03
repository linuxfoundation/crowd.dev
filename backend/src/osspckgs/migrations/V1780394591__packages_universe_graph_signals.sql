-- Graph-derived signals for criticality scoring (ADR-0001 §Criticality scoring methodology).
-- Populated by the criticality worker; NULL until first pass.

ALTER TABLE packages_universe
  ADD COLUMN IF NOT EXISTS transitive_dependent_count bigint,
  ADD COLUMN IF NOT EXISTS centrality_score           numeric(10, 8);

-- Split dependent_packages_count into direct (MinimumDepth=1) vs transitive (MinimumDepth>1).

ALTER TABLE packages
  RENAME COLUMN dependent_packages_count TO dependent_count;

ALTER TABLE packages
  ADD COLUMN IF NOT EXISTS transitive_dependent_count bigint;

ALTER TABLE packages_universe
  RENAME COLUMN dependent_packages_count TO dependent_count;

-- The renamed column holds old all-dependent counts (direct + transitive combined).
-- NULL them out so criticality scoring uses 0 for this signal until dependent_counts
-- is re-ingested with the split values; inflated scores are worse than missing ones.
UPDATE packages        SET dependent_count = NULL;
UPDATE packages_universe SET dependent_count = NULL;

-- rank_packages_universe() stores its body as text — renaming the column above does NOT
-- update the function. Replace it here so it references dependent_count, not the old name.
CREATE OR REPLACE FUNCTION rank_packages_universe(
    weight_downloads            numeric,
    weight_dependent_repos      numeric,
    weight_dependent_packages   numeric,
    log_smoothing               numeric,
    critical_top_n_by_ecosystem jsonb
)
RETURNS TABLE (scored_rows int, ranked_rows int, propagated_rows int)
LANGUAGE plpgsql
AS $$
DECLARE
    n_scored     int;
    n_ranked     int;
    n_propagated int;
BEGIN
    WITH new_scores AS (
        SELECT
            id,
            ( LN(log_smoothing + COALESCE(downloads_last_30d, 0))       * weight_downloads
            + LN(log_smoothing + COALESCE(dependent_repos_count, 0))    * weight_dependent_repos
            + LN(log_smoothing + COALESCE(dependent_count, 0))          * weight_dependent_packages
            )::numeric(10, 4) AS new_score
        FROM packages_universe
    )
    UPDATE packages_universe pu
    SET    criticality_score = ns.new_score
    FROM   new_scores ns
    WHERE  pu.id = ns.id
      AND  pu.criticality_score IS DISTINCT FROM ns.new_score;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    WITH ranked AS (
        SELECT
            id,
            ecosystem,
            ROW_NUMBER() OVER (
                PARTITION BY ecosystem
                ORDER BY criticality_score DESC NULLS LAST, id
            ) AS r
        FROM packages_universe
        WHERE purl IS NOT NULL
    ),
    with_flag AS (
        SELECT
            id,
            r,
            COALESCE(
                r <= (critical_top_n_by_ecosystem ->> ecosystem)::int,
                FALSE
            ) AS new_is_critical
        FROM ranked
    )
    UPDATE packages_universe pu
    SET    rank_in_ecosystem = wf.r,
           is_critical       = wf.new_is_critical
    FROM   with_flag wf
    WHERE  pu.id = wf.id
      AND  ( pu.rank_in_ecosystem IS DISTINCT FROM wf.r
          OR pu.is_critical       IS DISTINCT FROM wf.new_is_critical );

    GET DIAGNOSTICS n_ranked = ROW_COUNT;

    UPDATE packages p
    SET    criticality_score = pu.criticality_score
    FROM   packages_universe pu
    WHERE  p.purl = pu.purl
      AND  p.criticality_score IS DISTINCT FROM pu.criticality_score;

    GET DIAGNOSTICS n_propagated = ROW_COUNT;

    RETURN QUERY SELECT n_scored, n_ranked, n_propagated;
END;
$$;
