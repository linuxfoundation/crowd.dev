import { createReadStream } from 'node:fs'
import * as path from 'node:path'
import { createInterface } from 'node:readline'
import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { CopyStreamQuery, from as copyFrom } from 'pg-copy-streams'

import { QueryExecutor } from '@crowd/data-access-layer'
import { DbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'

import { LoadResult } from './types'

const log = getServiceChildLogger('cargo-load')

export const STAGING_SCHEMA = 'cargo_sync'

const COLUMN_TYPES: Record<string, Record<string, string>> = {
  crates: { id: 'integer' },
  versions: { crate_id: 'integer', id: 'integer' },
  default_versions: { crate_id: 'integer', num_versions: 'integer', version_id: 'integer' },
  version_downloads: { date: 'date', downloads: 'integer', version_id: 'integer' },
  dependencies: { crate_id: 'integer', id: 'integer', kind: 'integer', version_id: 'integer' },
  keywords: { crates_cnt: 'integer', id: 'integer' },
  crates_keywords: { crate_id: 'integer', keyword_id: 'integer' },
  crate_owners: { crate_id: 'integer', owner_id: 'integer', owner_kind: 'integer' },
  oauth_github: { user_id: 'integer' },
}

const REQUIRED_COLUMNS: Record<string, string[]> = {
  crates: ['id', 'name', 'description', 'homepage', 'repository'],
  versions: ['id', 'crate_id', 'created_at', 'num', 'license', 'yanked'],
  default_versions: ['crate_id', 'version_id'],
  version_downloads: ['date', 'downloads', 'version_id'],
  dependencies: ['crate_id', 'version_id', 'kind'],
  keywords: ['id', 'keyword'],
  crates_keywords: ['crate_id', 'keyword_id'],
  crate_owners: ['crate_id', 'owner_id', 'owner_kind'],
  oauth_github: ['user_id', 'login'],
}

async function readCsvHeader(csvPath: string): Promise<string[]> {
  const source = createReadStream(csvPath)
  const rl = createInterface({ input: source, crlfDelay: Infinity })
  try {
    for await (const line of rl) {
      return line.split(',').map((c) => c.trim())
    }
  } finally {
    rl.close()
    source.destroy()
  }
  throw new Error(`CSV has no header line: ${csvPath}`)
}

async function buildStagingTable(qx: QueryExecutor, table: string, csvPath: string): Promise<void> {
  const cols = await readCsvHeader(csvPath)
  const missing = (REQUIRED_COLUMNS[table] ?? []).filter((c) => !cols.includes(c))
  if (missing.length) {
    throw new Error(
      `cargo dump ${table}.csv is missing required column(s) [${missing.join(', ')}] — ` +
        `crates.io schema changed; header was [${cols.join(', ')}]`,
    )
  }
  const types = COLUMN_TYPES[table] ?? {}
  const ddl = cols.map((c) => `"${c.replace(/"/g, '""')}" ${types[c] ?? 'text'}`).join(', ')
  await qx.result(
    `DROP TABLE IF EXISTS ${STAGING_SCHEMA}.${table} CASCADE; CREATE TABLE ${STAGING_SCHEMA}.${table} (${ddl})`,
  )
}

// crates.csv needs NUL stripping — readme blobs contain 0x00 bytes that COPY rejects.
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
    // @types/pg-copy-streams Submittable doesn't match @types/pg's query overload.
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
    con.done(true) // destroy — a failed COPY can leave the client unusable
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

  -- Incremental: only crates newer than packages.latest_release_at (read before enrichPackages overwrites it).
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
  JOIN packages p                             ON p.id = cp.package_id
  JOIN ${STAGING_SCHEMA}.agg_version_stats vs ON vs.crate_id = v.crate_id
  LEFT JOIN ${STAGING_SCHEMA}.default_versions dv ON dv.crate_id = v.crate_id
  WHERE p.latest_release_at IS NULL OR vs.latest_release_at > p.latest_release_at;
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

  -- Audit scratch: enrich phases append (package_id, field); flushAudit aggregates per purl.
  DROP TABLE IF EXISTS ${STAGING_SCHEMA}.audit_changes CASCADE;
  CREATE TABLE ${STAGING_SCHEMA}.audit_changes (package_id bigint, field text);
`

export async function loadDump(
  qx: QueryExecutor,
  db: DbConnection,
  dumpDir: string,
): Promise<LoadResult> {
  const startedAt = Date.now()
  const dataDir = path.join(dumpDir, 'data')

  log.info('Creating staging schema...')
  await qx.result(`CREATE SCHEMA IF NOT EXISTS ${STAGING_SCHEMA}`)

  const counts: Record<string, number> = {}
  for (const { table, file, stripNul: doStripNul } of CSV_FILES) {
    const csvPath = path.join(dataDir, file)
    await buildStagingTable(qx, table, csvPath)
    counts[table] = await copyCsv(db, table, csvPath, doStripNul ?? false)
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

  // Prevent autoanalyze mid-aggregation and give the planner accurate stats.
  log.info('Analyzing staging tables...')
  await qx.result(`ANALYZE ${STAGING_SCHEMA}.crates, ${STAGING_SCHEMA}.versions,
                   ${STAGING_SCHEMA}.version_downloads, ${STAGING_SCHEMA}.dependencies,
                   ${STAGING_SCHEMA}.default_versions, ${STAGING_SCHEMA}.crate_owners,
                   ${STAGING_SCHEMA}.oauth_github, ${STAGING_SCHEMA}.crates_keywords,
                   ${STAGING_SCHEMA}.keywords`)

  // work_mem: hash joins stay in RAM. max_parallel_workers=0: parallel workers
  // contend on dsm over fresh tables and fail non-deterministically.
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
