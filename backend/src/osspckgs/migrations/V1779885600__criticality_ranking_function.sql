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
    -- Step 1: recompute scores; only touch rows whose score changed.
    -- log_smoothing is added before LN() to avoid LN(0) on zero-count rows
    -- and to compress the gap between small and large values (e.g. LN(1)=0
    -- vs LN(2)≈0.69 gives a gentler floor than LN(0)=-∞). Typically 1.0.
    WITH new_scores AS (
        SELECT
            id,
            ( LN(log_smoothing + COALESCE(downloads_30d, 0))            * weight_downloads
            + LN(log_smoothing + COALESCE(dependent_repos_count, 0))    * weight_dependent_repos
            + LN(log_smoothing + COALESCE(dependent_packages_count, 0)) * weight_dependent_packages
            )::numeric(10, 4) AS new_score
        FROM packages_universe
    )
    UPDATE packages_universe pu
    SET    criticality_score = ns.new_score,
           last_ranked_at    = NOW()
    FROM   new_scores ns
    WHERE  pu.id = ns.id
      AND  pu.criticality_score IS DISTINCT FROM ns.new_score;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    -- Step 2: rank within ecosystem; flag is_critical via JSONB lookup.
    -- Only purl-having rows are ranked (null purls can't propagate to packages).
    -- Tie-break by id keeps ranks deterministic across runs so IS DISTINCT FROM
    -- doesn't no-op-write equal-score rows on every call.
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

    -- Step 3: propagate criticality_score onto Tier-2 packages rows.
    UPDATE packages p
    SET    criticality_score = pu.criticality_score
    FROM   packages_universe pu
    WHERE  p.purl = pu.purl
      AND  p.criticality_score IS DISTINCT FROM pu.criticality_score;

    GET DIAGNOSTICS n_propagated = ROW_COUNT;

    RETURN QUERY SELECT n_scored, n_ranked, n_propagated;
END;
$$;
