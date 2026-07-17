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

// Top 5 active security contacts (highest score first) for a package's best repo link.
// Expects the `pr` alias from BEST_REPO_LINK_JOIN (pr.repo_id) in scope. Returns a JSON
// array or NULL when the repo has no contacts.
export const SECURITY_CONTACTS_SUBQUERY = `(
        SELECT json_agg(sc ORDER BY sc.score DESC)
        FROM (
          SELECT channel, value, role, confidence, score
          FROM security_contacts
          WHERE repo_id = pr.repo_id AND deleted_at IS NULL
          ORDER BY score DESC
          LIMIT 5
        ) sc
      )`

// Most recent 30-day download count for a package. Expects a `packages p` in the outer FROM clause.
export const DOWNLOADS_LAST_30D_SUBQUERY = `(
        SELECT d.count::text
        FROM downloads_last_30d d
        WHERE d.purl = p.purl
        ORDER BY d.end_date DESC
        LIMIT 1
      )`

// Resolution of an advisory against a package's latest version: 'patched' once every
// affected range is fixed/passed, 'open' otherwise, NULL when unknowable (no latest
// version, or no affected ranges recorded). Aggregate expression — must be used under a
// GROUP BY, and expects `packages p` and `advisory_affected_ranges ar` in scope.
// TODO: text comparison is lexicographic, not semver — '1.9.0' >= '1.10.0' is TRUE here.
// Replace with a proper semver comparison function when one is available in the DB.
export const ADVISORY_RESOLUTION_EXPR = `CASE
          WHEN p.latest_version IS NULL THEN NULL
          WHEN COUNT(ar.id) = 0 THEN NULL
          WHEN BOOL_AND(
            CASE
              WHEN ar.fixed_version IS NULL AND ar.last_affected IS NULL THEN FALSE
              WHEN ar.fixed_version IS NOT NULL AND p.latest_version >= ar.fixed_version THEN TRUE
              WHEN ar.fixed_version IS NOT NULL THEN FALSE
              WHEN ar.last_affected IS NOT NULL AND p.latest_version > ar.last_affected THEN TRUE
              ELSE FALSE
            END
          ) THEN 'patched'
          ELSE 'open'
        END`
