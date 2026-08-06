import { QueryExecutor } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { STAGING_SCHEMA } from './loadDump'
import {
  EnrichDownloadsDailyResult,
  EnrichMaintainersResult,
  EnrichPackagesResult,
  EnrichReposResult,
  EnrichVersionsResult,
} from './types'

const log = getServiceChildLogger('cargo-enrich')

export const AUDIT_WORKER = 'cargo-registry'
const INGESTION_SOURCE = 'cargo-registry'
const REPO_LINK_SOURCE = 'declared' // same convention as npm/maven for manifest-declared repo URLs
const REPO_LINK_CONFIDENCE = 0.8

// synchronous_commit off: skip WAL fsync on bulk writes — job is idempotent.
const WORK_MEM = '512MB'
const SYNC_COMMIT = 'off'

// Reads settings back after applying — fails loudly if they didn't take.
async function withTunedSession<T>(
  qx: QueryExecutor,
  phase: string,
  fn: (tx: QueryExecutor) => Promise<T>,
): Promise<T> {
  return qx.tx(async (tx) => {
    await tx.result(`SET LOCAL work_mem = '${WORK_MEM}'`)
    await tx.result(`SET LOCAL synchronous_commit = ${SYNC_COMMIT}`)
    const s = await tx.selectOne(
      `SELECT current_setting('work_mem') AS work_mem,
              current_setting('synchronous_commit') AS synchronous_commit`,
    )
    if (s.work_mem !== WORK_MEM || s.synchronous_commit !== SYNC_COMMIT) {
      throw new Error(`cargo enrich (${phase}) session settings not applied: ${JSON.stringify(s)}`)
    }
    log.info({ phase, ...s }, 'enrich session tuned')
    return fn(tx)
  })
}

// Nullable fields are COALESCEd — dump nulls don't wipe existing values.
export async function enrichPackages(qx: QueryExecutor): Promise<EnrichPackagesResult> {
  const row = await withTunedSession(qx, 'packages', (tx) =>
    tx.selectOne(
      `WITH snap AS (
         SELECT p.id, p.status, p.description, p.homepage, p.declared_repository_url,
                p.repository_url,
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
           repository_url          = CASE WHEN e.declared_repository_url IS NOT NULL
                                          THEN rn.repository_url ELSE p.repository_url END,
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
         LEFT JOIN ${STAGING_SCHEMA}.repo_norm rn ON rn.declared = e.declared_repository_url
         WHERE p.id = e.package_id
         RETURNING p.id
       ),
       diff AS (
         SELECT s.id AS package_id, f.field
         FROM snap s
         JOIN ${STAGING_SCHEMA}.enrich_packages e ON e.package_id = s.id
         LEFT JOIN ${STAGING_SCHEMA}.repo_norm rn ON rn.declared = e.declared_repository_url
         CROSS JOIN LATERAL (VALUES
           ('packages.status',                  s.status                  IS DISTINCT FROM e.status),
           ('packages.description',             s.description             IS DISTINCT FROM COALESCE(e.description, s.description)),
           ('packages.homepage',                s.homepage                IS DISTINCT FROM COALESCE(e.homepage, s.homepage)),
           ('packages.declared_repository_url', s.declared_repository_url IS DISTINCT FROM COALESCE(e.declared_repository_url, s.declared_repository_url)),
           ('packages.repository_url',          s.repository_url          IS DISTINCT FROM
               CASE WHEN e.declared_repository_url IS NOT NULL THEN rn.repository_url ELSE s.repository_url END),
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

// namespace/name from the package row; license stored as ARRAY[spdx_string].
export async function enrichVersions(qx: QueryExecutor): Promise<EnrichVersionsResult> {
  const row = await withTunedSession(qx, 'versions', (tx) =>
    tx.selectOne(
      `WITH old AS (
         SELECT v.package_id, v.number, v.published_at, v.is_latest, v.is_prerelease, v.licenses
         FROM versions v
         WHERE v.package_id IN (SELECT package_id FROM ${STAGING_SCHEMA}.enrich_versions)
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

// Writes only url + host — other repo fields belong to the GitHub enricher. Uses
// repo_norm (built by normalizeRepos) so repos.url/package_repos always agree with
// the canonical packages.repository_url — never the raw declared_repository_url.
export async function enrichRepos(qx: QueryExecutor): Promise<EnrichReposResult> {
  return withTunedSession(qx, 'repos', async (tx) => {
    const repoRow = await tx.selectOne(
      `WITH new_repos AS (
         INSERT INTO repos (url, host, updated_at)
         SELECT DISTINCT rn.repository_url, rn.host, NOW()
         FROM ${STAGING_SCHEMA}.enrich_packages e
         JOIN ${STAGING_SCHEMA}.repo_norm rn ON rn.declared = e.declared_repository_url
         ON CONFLICT (url) DO NOTHING
         RETURNING url
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT e.package_id, f.field
         FROM ${STAGING_SCHEMA}.enrich_packages e
         JOIN ${STAGING_SCHEMA}.repo_norm rn ON rn.declared = e.declared_repository_url
         JOIN new_repos nr ON nr.url = rn.repository_url
         CROSS JOIN LATERAL (VALUES ('repos.url'), ('repos.host')) AS f(field)
         RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM new_repos)::int AS repos`,
    )

    // Prunes stale 'declared' links before relinking — covers junk/unparseable declared
    // values, rewrites (declared URL now maps elsewhere), and removals (this dump's
    // declared_repository_url is NULL, meaning the crate no longer declares a repo at
    // all — loadDump.ts stages every matched crate every run, so NULL here is
    // authoritative, not "no data this run"). Safe to always prune on that signal because
    // the DELETE is scoped to source = 'declared' — cargo only ever removes links it owns.
    // Without this, package_repos would accumulate a link to a repo no crate declares
    // anymore, and consumers such as security-contacts (which join through
    // repos ⋈ package_repos, not packages.repository_url) would keep reading it.
    const pruneRow = await tx.selectOne(
      `WITH targets AS (
         SELECT e.package_id, r.id AS repo_id
         FROM ${STAGING_SCHEMA}.enrich_packages e
         LEFT JOIN ${STAGING_SCHEMA}.repo_norm rn ON rn.declared = e.declared_repository_url
         LEFT JOIN repos r ON r.url = rn.repository_url
       ),
       del AS (
         DELETE FROM package_repos pr
         USING targets t
         WHERE pr.package_id = t.package_id
           AND pr.source = $(source)
           AND (t.repo_id IS NULL OR pr.repo_id IS DISTINCT FROM t.repo_id)
         RETURNING pr.package_id
       ),
       ins_audit AS (
         INSERT INTO ${STAGING_SCHEMA}.audit_changes (package_id, field)
         SELECT package_id, 'package_repos.repo_id' FROM del
         RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM del)::int AS pruned`,
      { source: REPO_LINK_SOURCE },
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
         JOIN ${STAGING_SCHEMA}.repo_norm rn ON rn.declared = e.declared_repository_url
         JOIN repos r ON r.url = rn.repository_url
         -- Leaves source untouched on conflict — a link another enricher already owns for
         -- this (package_id, repo_id) keeps its provenance instead of being reassigned to
         -- 'declared', matching upsertMavenPackageRepo's confidence-only merge.
         ON CONFLICT (package_id, repo_id) DO UPDATE SET
           confidence  = GREATEST(EXCLUDED.confidence, package_repos.confidence),
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
         SELECT package_id, field FROM diff RETURNING 1
       )
       SELECT (SELECT COUNT(*) FROM ins)::int AS links`,
      { source: REPO_LINK_SOURCE, confidence: REPO_LINK_CONFIDENCE },
    )

    return { repos: repoRow.repos, links: linkRow.links, pruned: pruneRow.pruned }
  })
}

// Fully replaces each package's maintainer links (role='owner').
export async function enrichMaintainers(qx: QueryExecutor): Promise<EnrichMaintainersResult> {
  return withTunedSession(qx, 'maintainers', async (tx) => {
    await tx.result(
      `DROP TABLE IF EXISTS ${STAGING_SCHEMA}.mnt_before;
       CREATE TABLE ${STAGING_SCHEMA}.mnt_before AS
       SELECT pm.package_id, pm.maintainer_id
       FROM package_maintainers pm
       WHERE pm.package_id IN (SELECT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers)`,
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
       WHERE package_id IN (SELECT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers)`,
    )

    const links = await tx.result(
      `INSERT INTO package_maintainers (package_id, maintainer_id, role, created_at, updated_at)
       SELECT em.package_id, m.id, 'owner', NOW(), NOW()
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
          WHERE package_id IN (SELECT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers))
         UNION
         (SELECT package_id, maintainer_id FROM package_maintainers
          WHERE package_id IN (SELECT package_id FROM ${STAGING_SCHEMA}.enrich_maintainers)
          EXCEPT
          SELECT package_id, maintainer_id FROM ${STAGING_SCHEMA}.mnt_before)
       ) d`,
    )

    return { maintainers: mntRow.n, links }
  })
}

// Batched by date — downloads_daily is range-partitioned. DO NOTHING preserves history.
export async function enrichDownloadsDaily(qx: QueryExecutor): Promise<EnrichDownloadsDailyResult> {
  return withTunedSession(qx, 'downloadsDaily', async (tx) => {
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
