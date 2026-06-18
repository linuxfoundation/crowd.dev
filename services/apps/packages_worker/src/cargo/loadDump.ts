import { createReadStream } from 'node:fs'
import * as path from 'node:path'
import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { CopyStreamQuery, from as copyFrom } from 'pg-copy-streams'

import { QueryExecutor } from '@crowd/data-access-layer'
import { DbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'

import { LoadResult } from './types'

const log = getServiceChildLogger('cargo-load')

export const STAGING_SCHEMA = 'cargo_sync'

// Minimal staging schema: only the dump tables that feed an enriched field.
// Column order matches each CSV header exactly (cross-checked against the dump's
// import.sql) so `COPY ... FROM STDIN CSV HEADER` maps by position. IDs and the
// one date column are typed for fast joins; everything else is text. Each table
// is dropped first so a stale leftover from a crashed run can't collide.
const STAGING_DDL = `
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.crates CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.crates (
    created_at text, description text, documentation text, homepage text,
    id integer, max_features text, max_upload_size text, name text,
    readme text, repository text, trustpub_only text, updated_at text
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.versions CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.versions (
    bin_names text, categories text, checksum text, crate_id integer,
    crate_size text, created_at text, description text, documentation text,
    downloads text, edition text, features text, has_lib text, homepage text,
    id integer, keywords text, license text, links text, num text,
    num_no_build text, published_by text, repository text, rust_version text,
    updated_at text, yanked text
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.default_versions CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.default_versions (
    crate_id integer, num_versions integer, version_id integer
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.version_downloads CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.version_downloads (
    date date, downloads integer, version_id integer
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.dependencies CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.dependencies (
    crate_id integer, default_features text, explicit_name text, features text,
    id integer, kind integer, optional text, req text, target text,
    version_id integer
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.keywords CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.keywords (
    crates_cnt integer, created_at text, id integer, keyword text
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.crates_keywords CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.crates_keywords (
    crate_id integer, keyword_id integer
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.crate_owners CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.crate_owners (
    crate_id integer, created_at text, created_by text, owner_id integer,
    owner_kind integer
  );
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.oauth_github CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.oauth_github (
    account_id text, avatar text, login text, user_id integer
  );
`

// Load order. Only crates needs NUL stripping (readme blobs contain NUL bytes,
// which PostgreSQL COPY rejects in text data).
const CSV_FILES: Array<{ table: string; file: string; stripNul?: boolean }> = [
  { table: 'crates', file: 'crates.csv', stripNul: true },
  { table: 'versions', file: 'versions.csv' },
  { table: 'default_versions', file: 'default_versions.csv' },
  { table: 'version_downloads', file: 'version_downloads.csv' },
  { table: 'dependencies', file: 'dependencies.csv' },
  { table: 'keywords', file: 'keywords.csv' },
  { table: 'crates_keywords', file: 'crates_keywords.csv' },
  { table: 'crate_owners', file: 'crate_owners.csv' },
  { table: 'oauth_github', file: 'oauth_github.csv' },
]

// Drops NUL (0x00) bytes from a byte stream. Clean chunks pass through untouched.
const stripNul = (): Transform =>
  new Transform({
    transform(chunk: Buffer, _enc, cb) {
      cb(null, chunk.includes(0) ? chunk.filter((b) => b !== 0) : chunk)
    },
  })

async function copyCsv(
  db: DbConnection,
  table: string,
  csvPath: string,
  doStripNul: boolean,
): Promise<number> {
  const con = await db.connect()
  try {
    // The @types/pg COPY overload doesn't line up with @types/pg-copy-streams'
    // Submittable, so reach the raw client through `any` for this one call.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const stream: CopyStreamQuery = (con.client as any).query(
      copyFrom(`COPY ${STAGING_SCHEMA}.${table} FROM STDIN CSV HEADER`),
    )
    const source = createReadStream(csvPath)
    await (doStripNul ? pipeline(source, stripNul(), stream) : pipeline(source, stream))
    const rows = stream.rowCount
    con.done()
    return rows
  } catch (err) {
    // A failed COPY can leave the client unusable — destroy it (kill=true)
    // instead of returning it to the pool.
    con.done(true)
    throw new Error(`Failed to COPY ${table} from ${csvPath}: ${(err as Error).message}`)
  }
}

const STAGING_INDEXES = `
  CREATE INDEX ON ${STAGING_SCHEMA}.versions (crate_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.versions (id);
  CREATE INDEX ON ${STAGING_SCHEMA}.version_downloads (version_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.dependencies (version_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.default_versions (version_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.default_versions (crate_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.crates (id);
  CREATE INDEX ON ${STAGING_SCHEMA}.crate_owners (crate_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.oauth_github (user_id);
  CREATE INDEX ON ${STAGING_SCHEMA}.crates_keywords (crate_id);
`

// Per-crate scratch aggregates, grouped by the integer crate_id for fast hash
// joins over version_downloads (34M rows) and dependencies (28M rows). Both
// download aggregates use the same 30-day window as packages.downloads_last_30d.
const AGGREGATIONS = `
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.agg_downloads_30d CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.agg_downloads_30d AS
  SELECT v.crate_id, SUM(vd.downloads)::bigint AS downloads_30d
  FROM ${STAGING_SCHEMA}.version_downloads vd
  JOIN ${STAGING_SCHEMA}.versions v ON v.id = vd.version_id
  WHERE vd.date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY v.crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.agg_downloads_30d (crate_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.agg_downloads_daily CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.agg_downloads_daily AS
  SELECT v.crate_id, vd.date, SUM(vd.downloads)::bigint AS downloads
  FROM ${STAGING_SCHEMA}.version_downloads vd
  JOIN ${STAGING_SCHEMA}.versions v ON v.id = vd.version_id
  WHERE vd.date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY v.crate_id, vd.date;
  CREATE INDEX ON ${STAGING_SCHEMA}.agg_downloads_daily (crate_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.agg_dep_packages CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.agg_dep_packages AS
  SELECT d.crate_id AS target_crate_id, COUNT(DISTINCT dv.crate_id)::bigint AS dependent_packages
  FROM ${STAGING_SCHEMA}.default_versions dv
  JOIN ${STAGING_SCHEMA}.versions v ON v.id = dv.version_id AND v.yanked = 'f'
  JOIN ${STAGING_SCHEMA}.dependencies d ON d.version_id = dv.version_id AND d.kind = 0
  GROUP BY d.crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.agg_dep_packages (target_crate_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.agg_dep_repos CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.agg_dep_repos AS
  SELECT d.crate_id AS target_crate_id, COUNT(DISTINCT c.repository)::bigint AS dependent_repos
  FROM ${STAGING_SCHEMA}.default_versions dv
  JOIN ${STAGING_SCHEMA}.versions v ON v.id = dv.version_id AND v.yanked = 'f'
  JOIN ${STAGING_SCHEMA}.dependencies d ON d.version_id = dv.version_id AND d.kind = 0
  JOIN ${STAGING_SCHEMA}.crates c ON c.id = dv.crate_id
                                 AND c.repository IS NOT NULL AND c.repository <> ''
  GROUP BY d.crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.agg_dep_repos (target_crate_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.agg_keywords CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.agg_keywords AS
  SELECT ck.crate_id, array_agg(k.keyword ORDER BY k.keyword) AS keywords
  FROM ${STAGING_SCHEMA}.crates_keywords ck
  JOIN ${STAGING_SCHEMA}.keywords k ON k.id = ck.keyword_id
  GROUP BY ck.crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.agg_keywords (crate_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.agg_version_stats CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.agg_version_stats AS
  SELECT crate_id,
         COUNT(*)::integer            AS versions_count,
         MIN(created_at)::timestamptz AS first_release_at,
         MAX(created_at)::timestamptz AS latest_release_at
  FROM ${STAGING_SCHEMA}.versions
  GROUP BY crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.agg_version_stats (crate_id);
`

// Resolves each crate to its existing cargo package row (purl is unique on both
// sides, so the match is 1:1) and denormalizes the scratch aggregates into the
// package_id-keyed tables the enrich phases consume. Crates with no matching
// package drop out here — the enrich phases never recompute the purl or touch
// crates again. The purl normalization (lowercase, hyphen→underscore) matches
// how cargo packages are keyed in packages-db.
const DENORMALIZE = `
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.crate_package CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.crate_package AS
  SELECT c.id AS crate_id, p.id AS package_id
  FROM ${STAGING_SCHEMA}.crates c
  JOIN packages p ON p.ecosystem = 'cargo'
                  AND p.purl = 'pkg:cargo/' || LOWER(REPLACE(c.name, '-', '_'));
  CREATE INDEX ON ${STAGING_SCHEMA}.crate_package (crate_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.enrich_packages CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.enrich_packages AS
  SELECT
    cp.package_id,
    CASE WHEN v.yanked = 't' THEN 'deprecated' ELSE 'active' END AS status,
    NULLIF(c.description, '')  AS description,
    NULLIF(c.homepage, '')     AS homepage,
    NULLIF(c.repository, '')   AS declared_repository_url,
    CASE WHEN NULLIF(v.license, '') IS NOT NULL THEN ARRAY[v.license] END AS licenses,
    NULLIF(v.license, '')      AS licenses_raw,
    kw.keywords                AS keywords,
    vs.versions_count,
    v.num                      AS latest_version,
    vs.first_release_at,
    vs.latest_release_at,
    COALESCE(dp.dependent_packages, 0) AS dependent_count,
    COALESCE(dr.dependent_repos, 0)    AS dependent_repos_count,
    COALESCE(d30.downloads_30d, 0)     AS downloads_last_30d
  FROM ${STAGING_SCHEMA}.crate_package cp
  JOIN ${STAGING_SCHEMA}.crates c             ON c.id = cp.crate_id
  JOIN ${STAGING_SCHEMA}.default_versions dv  ON dv.crate_id = c.id
  JOIN ${STAGING_SCHEMA}.versions v           ON v.id = dv.version_id
  JOIN ${STAGING_SCHEMA}.agg_version_stats vs ON vs.crate_id = c.id
  LEFT JOIN ${STAGING_SCHEMA}.agg_downloads_30d d30 ON d30.crate_id = c.id
  LEFT JOIN ${STAGING_SCHEMA}.agg_dep_packages dp   ON dp.target_crate_id = c.id
  LEFT JOIN ${STAGING_SCHEMA}.agg_dep_repos dr      ON dr.target_crate_id = c.id
  LEFT JOIN ${STAGING_SCHEMA}.agg_keywords kw       ON kw.crate_id = c.id;
  CREATE INDEX ON ${STAGING_SCHEMA}.enrich_packages (package_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.enrich_versions CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.enrich_versions AS
  SELECT
    cp.package_id,
    v.num                                AS number,
    NULLIF(v.created_at, '')::timestamptz AS published_at,
    (v.id = dv.version_id)               AS is_latest,
    (v.num LIKE '%-%')                   AS is_prerelease,
    NULLIF(v.license, '')                AS license
  FROM ${STAGING_SCHEMA}.versions v
  JOIN ${STAGING_SCHEMA}.crate_package cp     ON cp.crate_id = v.crate_id
  LEFT JOIN ${STAGING_SCHEMA}.default_versions dv ON dv.crate_id = v.crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.enrich_versions (package_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.enrich_maintainers CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.enrich_maintainers AS
  SELECT DISTINCT cp.package_id, og.login AS github_login
  FROM ${STAGING_SCHEMA}.crate_owners co
  JOIN ${STAGING_SCHEMA}.crate_package cp ON cp.crate_id = co.crate_id
  JOIN ${STAGING_SCHEMA}.oauth_github og  ON og.user_id = co.owner_id
  WHERE co.owner_kind = 0;
  CREATE INDEX ON ${STAGING_SCHEMA}.enrich_maintainers (package_id);

  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.enrich_downloads_daily CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.enrich_downloads_daily AS
  SELECT cp.package_id, dd.date, dd.downloads
  FROM ${STAGING_SCHEMA}.agg_downloads_daily dd
  JOIN ${STAGING_SCHEMA}.crate_package cp ON cp.crate_id = dd.crate_id;
  CREATE INDEX ON ${STAGING_SCHEMA}.enrich_downloads_daily (date);

  -- Scratch sink for audit field-change names; each enrich phase appends
  -- (package_id, field) rows, flushAudit aggregates them per purl.
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.audit_changes CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.audit_changes (package_id bigint, field text);
`

// loadDump stages the crates.io dump CSVs into the cargo_sync schema, builds
// indexes, aggregates per-crate metrics, then denormalizes everything into the
// package_id-keyed enrich_* tables the enrichment phases consume. dumpDir is the
// extracted dump root; CSVs live under <dumpDir>/data. Safe to re-run: the schema
// is rebuilt from scratch on every call.
export async function loadDump(
  qx: QueryExecutor,
  db: DbConnection,
  dumpDir: string,
): Promise<LoadResult> {
  const startedAt = Date.now()
  const dataDir = path.join(dumpDir, 'data')

  log.info('Creating staging schema...')
  await qx.result(
    `CREATE SCHEMA IF NOT EXISTS ${STAGING_SCHEMA};
     ${STAGING_DDL}`,
  )

  const counts: Record<string, number> = {}
  for (const { table, file, stripNul: doStripNul } of CSV_FILES) {
    counts[table] = await copyCsv(db, table, path.join(dataDir, file), doStripNul ?? false)
    log.info({ table, rows: counts[table] }, 'Loaded staging table')
  }

  // Guard against a truncated/empty dump — COPY can succeed with zero rows.
  if (!counts.crates || !counts.versions) {
    throw new Error(
      `cargo dump load produced empty core tables (crates=${counts.crates}, versions=${counts.versions}) — dump may be truncated`,
    )
  }

  log.info('Building staging indexes...')
  await qx.result(STAGING_INDEXES)

  // Give the planner real stats on the freshly loaded tables before the heavy
  // joins, and pre-empt autoanalyze from firing mid-aggregation.
  log.info('Analyzing staging tables...')
  await qx.result(`ANALYZE ${STAGING_SCHEMA}.crates, ${STAGING_SCHEMA}.versions,
                   ${STAGING_SCHEMA}.version_downloads, ${STAGING_SCHEMA}.dependencies,
                   ${STAGING_SCHEMA}.default_versions, ${STAGING_SCHEMA}.crate_owners,
                   ${STAGING_SCHEMA}.oauth_github, ${STAGING_SCHEMA}.crates_keywords,
                   ${STAGING_SCHEMA}.keywords`)

  // Aggregations and denormalization share one transaction with a tuned work_mem
  // so the large hash joins/group-bys have enough memory on a single session.
  // Parallel gather is disabled: on a just-loaded dataset its workers contend for
  // shared-memory segments (dsm) and fail non-deterministically; serial is fine
  // for a daily batch.
  log.info('Aggregating and denormalizing...')
  let matched = 0
  await qx.tx(async (tx) => {
    await tx.result(`SET LOCAL work_mem = '512MB'`)
    await tx.result(`SET LOCAL max_parallel_workers_per_gather = 0`)
    await tx.result(AGGREGATIONS)
    await tx.result(DENORMALIZE)
    const row = await tx.selectOne(
      `SELECT COUNT(*)::int AS n FROM ${STAGING_SCHEMA}.enrich_packages`,
    )
    matched = row.n
  })

  const durationMs = Date.now() - startedAt
  log.info({ matched, durationMs }, 'Dump load complete')

  return {
    crates: counts.crates ?? 0,
    versions: counts.versions ?? 0,
    dependencies: counts.dependencies ?? 0,
    versionDownloads: counts.version_downloads ?? 0,
    owners: counts.crate_owners ?? 0,
    matched,
    durationMs,
  }
}
