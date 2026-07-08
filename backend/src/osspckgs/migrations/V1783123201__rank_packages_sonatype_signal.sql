-- Add the Sonatype popularity score as a 4th ranking signal in rank_packages().
--
-- Sonatype could not deliver raw Maven download counts, so they provide a
-- popularity score (0-100, normalized per ecosystem) as the substitute for the
-- missing downloads signal. Maven previously had only dependent_count and
-- transitive_dependent_count feeding criticality.
--
-- The signal slots into the existing cumulative-coverage machinery unchanged:
--   * cumulative-share is scale-invariant, so a 0-100 score sorts identically
--     to a raw count — no normalization needed;
--   * non-maven rows are all-NULL -> ecosystem total = 0 -> the WHERE clause
--     drops the signal for them, so npm/pypi ranking is untouched.
--
-- No tier short-circuit here (measurement-first): is_critical is a BOOL_OR, so
-- adding a signal can only make more packages critical, never fewer. Whether a
-- hard "P0 always critical" guarantee is needed is decided after measuring the
-- impact shift on real data.

CREATE OR REPLACE FUNCTION rank_packages(
    coverage_cutoff numeric DEFAULT 0.90,
    ecosystems      text[]  DEFAULT NULL
)
RETURNS TABLE(processed_rows int)
LANGUAGE plpgsql AS $$
DECLARE
    processed_count      int;
    effective_ecosystems text[];
BEGIN
    IF ecosystems IS NULL THEN
        SELECT ARRAY_AGG(DISTINCT ecosystem)
          INTO effective_ecosystems
          FROM packages;
    ELSE
        effective_ecosystems := ecosystems;
    END IF;

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
        -- One row per (package × signal). Signals with a zero ecosystem total are
        -- excluded so they don't factor into the average (e.g. downloads for maven,
        -- sonatype for npm/pypi).
        -- cumulative_share_exclusive for the top-ranked package equals 0 by arithmetic
        -- (sum of rows above it is 0), so the top package is always inside the critical set.
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
    UPDATE packages p
       SET impact             = final.new_impact,
           is_critical        = final.new_is_critical,
           rank_in_ecosystem  = final.new_rank_in_ecosystem,
           last_rank_pass_at  = NOW(),
           last_synced_at     = NOW()
      FROM final
     WHERE p.id = final.id;

    GET DIAGNOSTICS processed_count = ROW_COUNT;

    RETURN QUERY SELECT processed_count;
END;
$$;
