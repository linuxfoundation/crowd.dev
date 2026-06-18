import { QueryExecutor } from '@crowd/data-access-layer'

import { STAGING_SCHEMA } from './loadDump'
import {
  EnrichDownloadsDailyResult,
  EnrichMaintainersResult,
  EnrichPackagesResult,
  EnrichReposResult,
  EnrichVersionsResult,
} from './types'

export const AUDIT_WORKER = 'cargo-registry'
const INGESTION_SOURCE = 'cargo-registry'
// 'declared' + 0.8: same convention npm/maven use for a manifest-declared repo URL.
const REPO_LINK_SOURCE = 'declared'
const REPO_LINK_CONFIDENCE = 0.8
// Audit diffs join/DISTINCT over millions of rows (versions); the default 4MB
// spills to disk. Run each phase in a tx with a raised work_mem.
const WORK_MEM = '512MB'

function withWorkMem<T>(qx: QueryExecutor, fn: (tx: QueryExecutor) => Promise<T>): Promise<T> {
  return qx.tx(async (tx) => {
    await tx.result(`SET LOCAL work_mem = '${WORK_MEM}'`)
    return fn(tx)
  })
}

// Each phase writes its target and records changed field names into the
// audit_changes scratch table in one set-based statement; flushAudit aggregates
// them per purl. created_at/updated_at default to NOW(); freshness uses the
// columns each table already owns (last_synced_at, verified_at).

// Updates matched cargo package rows; nullable metadata is COALESCEd so a dump
// null can't wipe an existing value. repository_url is left to the GitHub enricher.
export async function enrichPackages(qx: QueryExecutor): Promise<EnrichPackagesResult> {
  const row = await withWorkMem(qx, (tx) =>
    tx.selectOne(
      `WITH snap AS (
         SELECT p.id, p.status, p.description, p.homepage, p.declared_repository_url,
                p.licenses, p.licenses_raw, p.keywords, p.versions_count, p.latest_version,
                p.first_release_at, p.latest_release_at, p.dependent_count,
                p.dependent_repos_count, p.downloads_last_30d
         FROM packages p
         JOIN ${STAGING_SCHEMA}.enrich_packages e ON e.package_id = p.id
       ),
       upd AS (
         UPDATE packages p SET
           status                  = e.status,
           description             = COALESCE(e.description, p.description),
           homepage                = COALESCE(e.homepage, p.homepage),
           declared_repository_url = COALESCE(e.declared_repository_url, p.declared_repository_url),
           licenses                = COALESCE(e.licenses, p.licenses),
           licenses_raw            = COALESCE(e.licenses_raw, p.licenses_raw),
           keywords                = COALESCE(e.keywords, p.keywords),
           versions_count          = e.versions_count,
           latest_version          = e.latest_version,
           first_release_at        = e.first_release_at,
           latest_release_at       = e.latest_release_at,
           dependent_count         = e.dependent_count,
           dependent_repos_count   = e.dependent_repos_count,
           downloads_last_30d      = e.downloads_last_30d,
           ingestion_source        = $(ingestionSource),
           last_synced_at          = NOW()
         FROM ${STAGING_SCHEMA}.enrich_packages e
         WHERE p.id = e.package_id
         RETURNING p.id
       ),
       diff AS (
         SELECT s.id AS package_id, f.field
         FROM snap s
         JOIN ${STAGING_SCHEMA}.enrich_packages e ON e.package_id = s.id
         CROSS JOIN LATERAL (VALUES
           ('packages.status',                  s.status                  IS DISTINCT FROM e.status),
           ('packages.description',             s.description             IS DISTINCT FROM COALESCE(e.description, s.description)),
           ('packages.homepage',                s.homepage                IS DISTINCT FROM COALESCE(e.homepage, s.homepage)),
           ('packages.declared_repository_url', s.declared_repository_url IS DISTINCT FROM COALESCE(e.declared_repository_url, s.declared_repository_url)),
           ('packages.licenses',                s.licenses                IS DISTINCT FROM COALESCE(e.licenses, s.licenses)),
           ('packages.licenses_raw',            s.licenses_raw            IS DISTINCT FROM COALESCE(e.licenses_raw, s.licenses_raw)),
           ('packages.keywords',                s.keywords                IS DISTINCT FROM COALESCE(e.keywords, s.keywords)),
           ('packages.versions_count',          s.versions_count          IS DISTINCT FROM e.versions_count),
           ('packages.latest_version',          s.latest_version          IS DISTINCT FROM e.latest_version),
           ('packages.first_release_at',        s.first_release_at        IS DISTINCT FROM e.first_release_at),
           ('packages.latest_release_at',       s.latest_release_at       IS DISTINCT FROM e.latest_release_at),
           ('packages.dependent_count',         s.dependent_count         IS DISTINCT FROM e.dependent_count),
           ('packages.dependent_repos_count',   s.dependent_repos_count   IS DISTINCT FROM e.dependent_repos_count),
           ('packages.downloads_last_30d',      s.downloads_last_30d      IS DISTINCT FROM e.downloads_last_30d)
         ) AS f(field, changed)
         WHERE f.changed
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT package_id, field FROM diff RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM upd)::int AS updated`,
      { ingestionSource: INGESTION_SOURCE },
    ),
  )
  return { updated: row.updated }
}

// Upserts every version of each matched crate. namespace/name come from the
// package row; the SPDX string is stored as a one-element text[]. Audits new
// versions and changed per-version fields.
export async function enrichVersions(qx: QueryExecutor): Promise<EnrichVersionsResult> {
  const row = await withWorkMem(qx, (tx) =>
    tx.selectOne(
      `WITH old AS (
         SELECT v.package_id, v.number, v.published_at, v.is_latest, v.is_prerelease, v.licenses
         FROM versions v
         WHERE v.package_id IN (SELECT DISTINCT package_id FROM ${STAGING_SCHEMA}.enrich_versions)
       ),
       ins AS (
         INSERT INTO versions (
           package_id, ecosystem, namespace, name, number,
           published_at, is_latest, is_prerelease, licenses, last_synced_at, created_at
         )
         SELECT e.package_id, 'cargo', p.namespace, p.name, e.number,
                e.published_at, e.is_latest, e.is_prerelease,
                CASE WHEN e.license IS NOT NULL THEN ARRAY[e.license] END, NOW(), NOW()
         FROM ${STAGING_SCHEMA}.enrich_versions e
         JOIN packages p ON p.id = e.package_id
         ON CONFLICT (package_id, number) DO UPDATE SET
           published_at   = COALESCE(EXCLUDED.published_at, versions.published_at),
           is_latest      = EXCLUDED.is_latest,
           is_prerelease  = EXCLUDED.is_prerelease,
           licenses       = COALESCE(EXCLUDED.licenses, versions.licenses),
           last_synced_at = NOW()
         RETURNING package_id, number, published_at, is_latest, is_prerelease, licenses
       ),
       diff AS (
         SELECT ins.package_id, f.field
         FROM ins
         LEFT JOIN old o ON o.package_id = ins.package_id AND o.number = ins.number
         CROSS JOIN LATERAL (VALUES
           ('versions.number',        o.number IS NULL),
           ('versions.published_at',  o.number IS NULL OR o.published_at  IS DISTINCT FROM ins.published_at),
           ('versions.is_latest',     o.number IS NULL OR o.is_latest     IS DISTINCT FROM ins.is_latest),
           ('versions.is_prerelease', o.number IS NULL OR o.is_prerelease IS DISTINCT FROM ins.is_prerelease),
           ('versions.licenses',      o.number IS NULL OR o.licenses      IS DISTINCT FROM ins.licenses)
         ) AS f(field, changed)
         WHERE f.changed
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT DISTINCT package_id, field FROM diff RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM ins)::int AS upserted`,
    ),
  )
  return { upserted: row.upserted }
}

// Writes only url + sanitized host (the rest of the repos row is the GitHub
// enricher's) and links each package to it. Audits new repos and new/changed links.
export async function enrichRepos(qx: QueryExecutor): Promise<EnrichReposResult> {
  return withWorkMem(qx, async (tx) => {
    const repoRow = await tx.selectOne(
      `WITH new_repos AS (
         INSERT INTO repos (url, host, updated_at)
         SELECT DISTINCT e.declared_repository_url,
           CASE
             WHEN e.declared_repository_url ~* '://([^/]+\\.)?github\\.com(/|$)'    THEN 'github'
             WHEN e.declared_repository_url ~* '://[^/]*gitlab'                     THEN 'gitlab'
             WHEN e.declared_repository_url ~* '://([^/]+\\.)?bitbucket\\.org(/|$)' THEN 'bitbucket'
             ELSE 'other'
           END,
           NOW()
         FROM ${STAGING_SCHEMA}.enrich_packages e
         WHERE e.declared_repository_url IS NOT NULL AND e.declared_repository_url LIKE 'http%'
         ON CONFLICT (url) DO NOTHING
         RETURNING url
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT e.package_id, f.field
         FROM ${STAGING_SCHEMA}.enrich_packages e
         JOIN new_repos nr ON nr.url = e.declared_repository_url
         CROSS JOIN LATERAL (VALUES ('repos.url'), ('repos.host')) AS f(field)
         RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM new_repos)::int AS repos`,
    )

    const linkRow = await tx.selectOne(
      `WITH old AS (
         SELECT pr.package_id, pr.repo_id, pr.source, pr.confidence
         FROM package_repos pr
         WHERE pr.package_id IN (
           SELECT package_id FROM ${STAGING_SCHEMA}.enrich_packages WHERE declared_repository_url IS NOT NULL
         )
       ),
       ins AS (
         INSERT INTO package_repos (package_id, repo_id, source, confidence, created_at, verified_at)
         SELECT e.package_id, r.id, $(source), $(confidence), NOW(), NOW()
         FROM ${STAGING_SCHEMA}.enrich_packages e
         JOIN repos r ON r.url = e.declared_repository_url
         WHERE e.declared_repository_url IS NOT NULL
         ON CONFLICT (package_id, repo_id) DO UPDATE SET
           source      = EXCLUDED.source,
           confidence  = EXCLUDED.confidence,
           verified_at = NOW()
         RETURNING package_id, repo_id, source, confidence
       ),
       diff AS (
         SELECT ins.package_id, f.field
         FROM ins
         LEFT JOIN old o ON o.package_id = ins.package_id AND o.repo_id = ins.repo_id
         CROSS JOIN LATERAL (VALUES
           ('package_repos.repo_id',    o.repo_id IS NULL),
           ('package_repos.source',     o.repo_id IS NULL OR o.source     IS DISTINCT FROM ins.source),
           ('package_repos.confidence', o.repo_id IS NULL OR o.confidence IS DISTINCT FROM ins.confidence)
         ) AS f(field, changed)
         WHERE f.changed
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT DISTINCT package_id, field FROM diff RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM ins)::int AS links`,
      { source: REPO_LINK_SOURCE, confidence: REPO_LINK_CONFIDENCE },
    )

    return { repos: repoRow.repos, links: linkRow.links }
  })
}

// Upserts owners as cargo maintainers (keyed by github login) and fully replaces
// each package's links. role is always 'owner'. Audits new/changed maintainers
// (fanned to their packages) and membership changes.
export async function enrichMaintainers(qx: QueryExecutor): Promise<EnrichMaintainersResult> {
  return withWorkMem(qx, async (tx) => {
    await tx.result(
      `DROP TABLE IF EXISTS ${STAGING_SCHEMA}.mnt_before;
       CREATE TABLE ${STAGING_SCHEMA}.mnt_before AS
       SELECT pm.package_id, pm.maintainer_id
       FROM package_maintainers pm
       WHERE pm.package_id IN (SELECT DISTINCT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers)`,
    )

    const mntRow = await tx.selectOne(
      `WITH old AS (SELECT username, github_login, url FROM maintainers WHERE ecosystem = 'cargo'),
       ins AS (
         INSERT INTO maintainers (ecosystem, username, github_login, url, created_at, updated_at)
         SELECT DISTINCT 'cargo', em.github_login, em.github_login,
                'https://github.com/' || em.github_login, NOW(), NOW()
         FROM ${STAGING_SCHEMA}.enrich_maintainers em
         ON CONFLICT (ecosystem, username) DO UPDATE SET
           github_login = COALESCE(EXCLUDED.github_login, maintainers.github_login),
           url          = COALESCE(EXCLUDED.url, maintainers.url),
           updated_at   = NOW()
         RETURNING username, github_login, url
       ),
       changed AS (
         SELECT ins.username, f.field
         FROM ins
         LEFT JOIN old o ON o.username = ins.username
         CROSS JOIN LATERAL (VALUES
           ('maintainers.github_login', o.username IS NULL OR o.github_login IS DISTINCT FROM ins.github_login),
           ('maintainers.url',          o.username IS NULL OR o.url          IS DISTINCT FROM ins.url)
         ) AS f(field, ch)
         WHERE f.ch
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT DISTINCT em.package_id, c.field
         FROM changed c
         JOIN ${STAGING_SCHEMA}.enrich_maintainers em ON em.github_login = c.username
         RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM ins)::int AS n`,
    )

    await tx.result(
      `DELETE FROM package_maintainers
       WHERE package_id IN (SELECT DISTINCT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers)`,
    )

    const links = await tx.result(
      `INSERT INTO package_maintainers (package_id, maintainer_id, role, created_at, updated_at)
       SELECT DISTINCT em.package_id, m.id, 'owner', NOW(), NOW()
       FROM ${STAGING_SCHEMA}.enrich_maintainers em
       JOIN maintainers m ON m.ecosystem = 'cargo' AND m.username = em.github_login
       ON CONFLICT (package_id, maintainer_id) DO NOTHING`,
    )

    // Membership delta (symmetric difference of before vs after) → audit.
    await tx.result(
      `INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
       SELECT DISTINCT package_id, 'package_maintainers.maintainer_id'
       FROM (
         (SELECT package_id, maintainer_id FROM ${STAGING_SCHEMA}.mnt_before
          EXCEPT
          SELECT package_id, maintainer_id FROM package_maintainers
          WHERE package_id IN (SELECT DISTINCT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers))
         UNION
         (SELECT package_id, maintainer_id FROM package_maintainers
          WHERE package_id IN (SELECT DISTINCT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers)
          EXCEPT
          SELECT package_id, maintainer_id FROM ${STAGING_SCHEMA}.mnt_before)
       ) d`,
    )

    return { maintainers: mntRow.n, links }
  })
}

// Inserts per-day download totals, batched by date (downloads_daily is range-
// partitioned by date). DO NOTHING preserves recorded history; new days audited.
export async function enrichDownloadsDaily(qx: QueryExecutor): Promise<EnrichDownloadsDailyResult> {
  return withWorkMem(qx, async (tx) => {
    const dates: Array<{ date: string }> = await tx.select(
      `SELECT DISTINCT date::text AS date FROM ${STAGING_SCHEMA}.enrich_downloads_daily ORDER BY date`,
    )
    let inserted = 0
    for (const { date } of dates) {
      const row = await tx.selectOne(
        `WITH ins AS (
           INSERT INTO downloads_daily (package_id, date, count, created_at, updated_at)
           SELECT package_id, date, downloads, NOW(), NOW()
           FROM ${STAGING_SCHEMA}.enrich_downloads_daily
           WHERE date = $(date)::date
           ON CONFLICT (package_id, date) DO NOTHING
           RETURNING package_id
         ),
         ins_audit AS (
           INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
           SELECT package_id, f.field FROM ins
           CROSS JOIN LATERAL (VALUES ('downloads_daily.date'), ('downloads_daily.count')) AS f(field)
           RETURNING 1
         )
         SELECT (SELECT COUNT(*) FROM ins)::int AS inserted`,
        { date },
      )
      inserted += row.inserted
    }
    return { inserted }
  })
}

// Aggregates the staged field-change names into audit_field_changes — one row
// per purl with the union of changed fields across all phases.
export async function flushAudit(qx: QueryExecutor): Promise<number> {
  return qx.result(
    `INSERT INTO audit_field_changes (worker, purl, changed_fields)
     SELECT $(worker), p.purl, array_agg(DISTINCT ac.field)
     FROM ${STAGING_SCHEMA}.audit_changes ac
     JOIN packages p ON p.id = ac.package_id
     GROUP BY p.purl`,
    { worker: AUDIT_WORKER },
  )
}
