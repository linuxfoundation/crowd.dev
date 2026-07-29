import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

export async function updateCollaborationSignal(qx: QueryExecutor): Promise<number> {
  return qx.result(
    `
    WITH critical_repos AS (
      SELECT DISTINCT pr.repo_id
      FROM package_repos pr
      JOIN packages p ON p.id = pr.package_id AND p.is_critical
    ),
    advisory_repos AS (
      SELECT
        pr.repo_id,
        BOOL_OR(EXISTS (
          SELECT 1 FROM advisory_affected_ranges rg
          WHERE rg.advisory_package_id = ap.id
            AND rg.fixed_version IS NOT NULL
            AND rg.deleted_at IS NULL
        ))                                                        AS any_fixed,
        BOOL_OR(a.published_at < NOW() - INTERVAL '90 days')      AS any_old
      FROM advisory_packages ap
      JOIN advisories a     ON a.id = ap.advisory_id
      JOIN package_repos pr ON pr.package_id = ap.package_id
      WHERE ap.package_id IS NOT NULL
      GROUP BY pr.repo_id
    ),
    components AS (
      SELECT
        cr.repo_id,
        (r.archived IS TRUE OR r.disabled IS TRUE)                AS inactive,
        CASE
          WHEN s.issues_opened_last_12m >= 5 THEN
            CASE
              WHEN s.issue_median_time_to_first_response_hours IS NULL THEN 0.0
              WHEN s.issue_median_time_to_first_response_hours <= 72  THEN 1.0
              WHEN s.issue_median_time_to_first_response_hours <= 336 THEN 0.5
              ELSE 0.0
            END
        END                                                       AS comp_r,
        CASE
          WHEN s.external_prs_opened_12m IS NOT NULL THEN
            CASE
              WHEN s.external_prs_opened_12m >= 3
              THEN s.external_prs_merged_12m::numeric / s.external_prs_opened_12m
            END
          WHEN s.prs_opened_last_12m >= 3
          THEN s.prs_merged_last_12m::numeric / s.prs_opened_last_12m
        END                                                       AS comp_m,
        CASE
          WHEN ar.any_fixed THEN 1.0
          WHEN ar.any_old   THEN 0.0
        END                                                       AS comp_a
      FROM critical_repos cr
      JOIN repos r ON r.id = cr.repo_id
      LEFT JOIN repo_activity_snapshot s ON s.repo_id = cr.repo_id
      LEFT JOIN advisory_repos ar        ON ar.repo_id = cr.repo_id
    ),
    scored AS (
      SELECT
        repo_id,
        inactive,
        ROUND(
          (COALESCE(comp_r, 0) + COALESCE(comp_m, 0) + COALESCE(comp_a, 0))
          / NULLIF((comp_r IS NOT NULL)::int + (comp_m IS NOT NULL)::int
                 + (comp_a IS NOT NULL)::int, 0)
          * 100
        )::int                                                    AS score
      FROM components
    ),
    final AS (
      SELECT
        repo_id,
        CASE WHEN inactive THEN NULL ELSE score END               AS new_score,
        CASE
          WHEN inactive        THEN 'inactive'
          WHEN score IS NULL   THEN 'unknown'
          WHEN score >= 70     THEN 'responsive'
          WHEN score >= 40     THEN 'mixed'
          ELSE                      'unresponsive'
        END                                                       AS new_tier
      FROM scored
    )
    UPDATE repos r
    SET collaboration_score = f.new_score,
        collaboration_tier  = f.new_tier,
        updated_at          = NOW()
    FROM final f
    WHERE r.id = f.repo_id
      AND (r.collaboration_score, r.collaboration_tier)
          IS DISTINCT FROM (f.new_score, f.new_tier)
    `,
    {},
  )
}
