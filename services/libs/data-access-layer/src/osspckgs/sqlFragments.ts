// Severity stored as uppercase in advisories table.
// Ranks: CRITICAL=4, HIGH=3, MEDIUM=2, LOW=1
export const SEVERITY_RANK_EXPR = `MAX(CASE a.severity
    WHEN 'CRITICAL' THEN 4
    WHEN 'HIGH'     THEN 3
    WHEN 'MEDIUM'   THEN 2
    WHEN 'LOW'      THEN 1
    ELSE 0 END)::int`

export const STEWARD_MENTIONED_JOIN = `
  LEFT JOIN stewards st_mentioned
    ON sa.activity_type = 'steward_added'
    AND st_mentioned.user_id = (sa.metadata->>'userId')`

export const STEWARD_DISPLAY_NAME_METADATA = `CASE
        WHEN sa.activity_type = 'steward_added' AND st_mentioned.display_name IS NOT NULL
        THEN COALESCE(sa.metadata, '{}'::jsonb) || jsonb_build_object('stewardDisplayName', st_mentioned.display_name)
        ELSE sa.metadata
      END`

// Best repo link for a package: highest-confidence package_repos row, preferring
// a 'declared' source on ties. Expects a `packages p` in the outer FROM clause.
export const BEST_REPO_LINK_JOIN = `LEFT JOIN LATERAL (
      SELECT pr2.repo_id, pr2.confidence
      FROM package_repos pr2
      WHERE pr2.package_id = p.id
      ORDER BY pr2.confidence DESC, (pr2.source = 'declared') DESC
      LIMIT 1
    ) pr ON true
    LEFT JOIN repos r ON r.id = pr.repo_id`

// Expects a `packages p` in the outer FROM clause.
export const MAINTAINER_COUNT_SUBQUERY = `(SELECT COUNT(*)::int FROM package_maintainers pm WHERE pm.package_id = p.id)`

// Most recent 30-day download count for a package. Expects a `packages p` in the outer FROM clause.
export const DOWNLOADS_LAST_30D_SUBQUERY = `(
        SELECT d.count::text
        FROM downloads_last_30d d
        WHERE d.purl = p.purl
        ORDER BY d.end_date DESC
        LIMIT 1
      )`
