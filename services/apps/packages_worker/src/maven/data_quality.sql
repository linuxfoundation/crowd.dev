-- ────────────────────────────────────────────────────────────────────────────
-- Maven enrichment — data quality scorecard
--
-- One read-only statement. Returns one row per check so it works in any SQL
-- client (psql, DBeaver, prod read-replica) and via validateDataQuality.ts.
--
-- Scope: the CRITICAL Maven set (packages_universe.is_critical = true) — i.e. the
-- set the POM fetcher is responsible for. Coverage % is over the whole critical
-- universe, so packages not yet enriched count as "not covered".
--
-- Columns:
--   section | metric | value | pct (of critical) | status
--   status: OK / LOW / POOR for coverage; OK / FAIL for anomalies & integrity.
--   Coverage thresholds (80% / 50%) are deliberately simple — tune as needed.
-- ────────────────────────────────────────────────────────────────────────────

WITH crit AS (
  -- denominator: the critical universe
  SELECT pu.purl
  FROM packages_universe pu
  WHERE pu.ecosystem = 'maven'
    AND pu.is_critical = true
    AND pu.purl IS NOT NULL
),
crit_n AS (SELECT count(*)::numeric AS n FROM crit),

-- per-package version aggregates (only for packages we care about)
ver AS (
  SELECT
    v.package_id,
    count(*)                                              AS versions_count,
    count(*) FILTER (WHERE v.is_latest)                   AS latest_count,
    count(*) FILTER (WHERE v.is_latest AND v.is_prerelease) AS latest_prerelease_count,
    max(v.number) FILTER (WHERE v.is_latest)              AS latest_number
  FROM versions v
  GROUP BY v.package_id
),

-- enriched critical packages with derived quality flags
pkg AS (
  SELECT
    pk.id,
    pk.ingestion_source,
    pk.repository_url,
    pk.latest_version,
    pk.last_synced_at,
    (pk.repository_url IS NOT NULL)                                   AS has_repo,
    (pk.licenses IS NOT NULL AND array_length(pk.licenses, 1) >= 1)   AS has_license,
    (pk.description IS NOT NULL AND length(btrim(pk.description)) > 0) AS has_description,
    (pk.homepage IS NOT NULL)                                         AS has_homepage,
    COALESCE(ver.versions_count, 0)                                   AS versions_count,
    COALESCE(ver.latest_count, 0)                                     AS latest_count,
    COALESCE(ver.latest_prerelease_count, 0)                          AS latest_prerelease_count,
    ver.latest_number,
    (pm.package_id IS NOT NULL)                                       AS has_maintainer
  FROM packages pk
  JOIN crit c ON c.purl = pk.purl
  LEFT JOIN ver ON ver.package_id = pk.id
  LEFT JOIN (SELECT DISTINCT package_id FROM package_maintainers) pm ON pm.package_id = pk.id
),

-- all critical-scoped counts in a single row
agg AS (
  SELECT
    (SELECT n FROM crit_n)                                                            AS critical_total,
    count(*)                                                                          AS enriched,
    count(*) FILTER (WHERE ingestion_source = 'maven-registry')                       AS src_registry,
    count(*) FILTER (WHERE ingestion_source = 'maven_not_on_central')                 AS src_not_central,
    count(*) FILTER (WHERE ingestion_source = 'maven_no_version')                      AS src_no_version,
    count(*) FILTER (WHERE ingestion_source = 'maven_error')                           AS src_error,
    count(*) FILTER (WHERE ingestion_source = 'packages_universe')                     AS src_universe,
    count(*) FILTER (WHERE has_repo)                                                   AS has_repo,
    count(*) FILTER (WHERE has_license)                                                AS has_license,
    count(*) FILTER (WHERE versions_count > 0)                                         AS has_versions,
    count(*) FILTER (WHERE has_maintainer)                                             AS has_maintainer,
    count(*) FILTER (WHERE has_description)                                            AS has_description,
    count(*) FILTER (WHERE has_homepage)                                              AS has_homepage,
    count(*) FILTER (WHERE latest_count > 1)                                           AS multi_latest,
    count(*) FILTER (WHERE versions_count > 0 AND latest_count = 0)                    AS no_latest,
    count(*) FILTER (WHERE latest_count = 1
                     AND latest_number IS DISTINCT FROM latest_version)               AS latest_mismatch,
    count(*) FILTER (WHERE latest_prerelease_count > 0)                                AS prerelease_latest,
    count(*) FILTER (WHERE has_repo AND repository_url !~ '^https?://')                AS repo_not_http,
    count(*) FILTER (WHERE last_synced_at >= now() - interval '24 hours')              AS synced_24h,
    count(*) FILTER (WHERE last_synced_at <  now() - interval '24 hours'
                     AND last_synced_at >= now() - interval '7 days')                  AS synced_week,
    count(*) FILTER (WHERE last_synced_at <  now() - interval '7 days')                AS synced_old
  FROM pkg
),

-- global integrity (not limited to the critical set)
integ AS (
  SELECT
    (SELECT count(*) FROM (SELECT purl FROM packages GROUP BY purl HAVING count(*) > 1) d)         AS dup_purl,
    (SELECT count(*) FROM versions v             LEFT JOIN packages p ON p.id = v.package_id WHERE p.id IS NULL) AS orphan_versions,
    (SELECT count(*) FROM package_repos pr       LEFT JOIN packages p ON p.id = pr.package_id WHERE p.id IS NULL) AS orphan_repos,
    (SELECT count(*) FROM package_maintainers pm LEFT JOIN packages p ON p.id = pm.package_id WHERE p.id IS NULL) AS orphan_maintainers,
    (SELECT count(*) FROM maintainers WHERE username IS NULL OR btrim(username) = '')               AS maintainer_no_username
),

-- coverage % + status helper
cov AS (
  SELECT
    metric, value,
    round(100.0 * value / nullif((SELECT critical_total FROM agg), 0), 1) AS pct
  FROM (VALUES
    ('enriched (row exists)',     (SELECT enriched        FROM agg)),
    ('ingestion=maven-registry',  (SELECT src_registry    FROM agg)),
    ('has repository_url',        (SELECT has_repo         FROM agg)),
    ('has license',               (SELECT has_license      FROM agg)),
    ('has versions',              (SELECT has_versions     FROM agg)),
    ('has maintainer',            (SELECT has_maintainer   FROM agg)),
    ('has description',           (SELECT has_description  FROM agg)),
    ('has homepage',              (SELECT has_homepage     FROM agg))
  ) AS t(metric, value)
)

-- ─── report ───────────────────────────────────────────────────────────────────
SELECT ord, section, metric, value, pct_txt AS pct, status FROM (
  -- totals
  SELECT 0 AS ord, '1. TOTALS' AS section, 'critical packages (universe)' AS metric,
         (SELECT critical_total FROM agg) AS value, '100.0%' AS pct_txt, 'INFO' AS status
  UNION ALL
  SELECT 1, '1. TOTALS', 'never enriched (no packages row)',
         (SELECT critical_total - enriched FROM agg),
         to_char(round(100.0 * (SELECT critical_total - enriched FROM agg)
                       / nullif((SELECT critical_total FROM agg), 0), 1), 'FM990.0') || '%',
         CASE WHEN (SELECT critical_total - enriched FROM agg) = 0 THEN 'OK' ELSE 'INFO' END

  -- coverage
  UNION ALL
  SELECT 10 + row_number() OVER (ORDER BY metric), '2. COVERAGE', metric, value,
         to_char(pct, 'FM990.0') || '%',
         CASE WHEN pct >= 80 THEN 'OK' WHEN pct >= 50 THEN 'LOW' ELSE 'POOR' END
  FROM cov

  -- ingestion_source breakdown (enriched rows)
  UNION ALL SELECT 30, '3. INGESTION SOURCE', 'maven-registry (full POM ok)', (SELECT src_registry    FROM agg), NULL, 'INFO'
  UNION ALL SELECT 31, '3. INGESTION SOURCE', 'maven_not_on_central',         (SELECT src_not_central FROM agg), NULL, 'INFO'
  UNION ALL SELECT 32, '3. INGESTION SOURCE', 'maven_no_version',             (SELECT src_no_version  FROM agg), NULL, 'INFO'
  UNION ALL SELECT 33, '3. INGESTION SOURCE', 'maven_error',                  (SELECT src_error       FROM agg), NULL,
         CASE WHEN (SELECT src_error FROM agg) = 0 THEN 'OK' ELSE 'WARN' END
  UNION ALL SELECT 34, '3. INGESTION SOURCE', 'packages_universe (unexpected on critical)', (SELECT src_universe FROM agg), NULL,
         CASE WHEN (SELECT src_universe FROM agg) = 0 THEN 'OK' ELSE 'WARN' END

  -- anomalies (expect 0)
  UNION ALL SELECT 40, '4. ANOMALIES (expect 0)', 'packages with >1 is_latest version',        (SELECT multi_latest      FROM agg), NULL, CASE WHEN (SELECT multi_latest      FROM agg)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 41, '4. ANOMALIES (expect 0)', 'has versions but no is_latest',             (SELECT no_latest         FROM agg), NULL, CASE WHEN (SELECT no_latest         FROM agg)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 42, '4. ANOMALIES (expect 0)', 'latest_version != is_latest version',       (SELECT latest_mismatch   FROM agg), NULL, CASE WHEN (SELECT latest_mismatch   FROM agg)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 43, '4. ANOMALIES (expect 0)', 'prerelease flagged as latest',              (SELECT prerelease_latest FROM agg), NULL, CASE WHEN (SELECT prerelease_latest FROM agg)=0 THEN 'OK' ELSE 'WARN' END
  UNION ALL SELECT 44, '4. ANOMALIES (expect 0)', 'repository_url not http(s)',                (SELECT repo_not_http     FROM agg), NULL, CASE WHEN (SELECT repo_not_http     FROM agg)=0 THEN 'OK' ELSE 'WARN' END

  -- global integrity (expect 0)
  UNION ALL SELECT 50, '5. INTEGRITY (expect 0)', 'duplicate purls in packages',     (SELECT dup_purl              FROM integ), NULL, CASE WHEN (SELECT dup_purl              FROM integ)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 51, '5. INTEGRITY (expect 0)', 'orphan versions',                 (SELECT orphan_versions       FROM integ), NULL, CASE WHEN (SELECT orphan_versions       FROM integ)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 52, '5. INTEGRITY (expect 0)', 'orphan package_repos',            (SELECT orphan_repos          FROM integ), NULL, CASE WHEN (SELECT orphan_repos          FROM integ)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 53, '5. INTEGRITY (expect 0)', 'orphan package_maintainers',      (SELECT orphan_maintainers    FROM integ), NULL, CASE WHEN (SELECT orphan_maintainers    FROM integ)=0 THEN 'OK' ELSE 'FAIL' END
  UNION ALL SELECT 54, '5. INTEGRITY (expect 0)', 'maintainers without username',    (SELECT maintainer_no_username FROM integ), NULL, CASE WHEN (SELECT maintainer_no_username FROM integ)=0 THEN 'OK' ELSE 'FAIL' END

  -- freshness (enriched critical)
  UNION ALL SELECT 60, '6. FRESHNESS', 'synced in last 24h',        (SELECT synced_24h  FROM agg), NULL, 'INFO'
  UNION ALL SELECT 61, '6. FRESHNESS', 'synced 24h–7d ago',         (SELECT synced_week FROM agg), NULL, 'INFO'
  UNION ALL SELECT 62, '6. FRESHNESS', 'synced > 7d ago (stale)',   (SELECT synced_old  FROM agg), NULL,
         CASE WHEN (SELECT synced_old FROM agg) = 0 THEN 'OK' ELSE 'WARN' END
) report
ORDER BY ord;
