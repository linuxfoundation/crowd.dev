import { QueryExecutor } from '../queryExecutor'

// Database access layer for OSV advisory ingestion and the critical-flag
// derive pass. All advisories / advisory_packages / advisory_affected_ranges
// queries used by services/apps/packages_worker live here; the worker keeps
// the parsing and version-matching logic.
//
// Tables are defined in backend/src/osspckgs/migrations/V1779710880__initial_schema.sql.
// Conventions (ecosystem case, package_name composition, range tuple
// uniqueness) follow ADR-0001 §OSV.

// ---- input shapes ----

// Mirrors the writable columns of the `advisories` row produced by the OSV
// parser. cvss_source is a closed enum (see CvssSource in the worker's
// osv/types.ts) but is typed loosely here so DAL doesn't depend on the
// parser's typing.
export interface AdvisoryUpsertInput {
  osvId: string
  source: string
  sourceUrl: string | null
  aliases: string[]
  severity: string | null
  cvss: number | null
  cvssSource: string | null
  summary: string | null
  details: string | null
  publishedAt: string | null
  modifiedAt: string | null
}

export interface AdvisoryPackageUpsertInput {
  advisoryId: number
  packageId: number | null
  ecosystem: string
  packageName: string
}

export interface AdvisoryRangeInsertInput {
  advisoryPackageId: number
  introducedVersion: string | null
  fixedVersion: string | null
  lastAffected: string | null
}

export interface PackageLookupInput {
  ecosystem: string
  namespace: string | null
  name: string
}

// ---- query result shapes ----

export interface PackageRow {
  id: number
  ecosystem: string
  latest_version: string
}

export interface RangeRow {
  pkg_id: number
  introduced_version: string | null
  fixed_version: string | null
  last_affected: string | null
  osv_id: string
}

// ---- upsert path (services/apps/packages_worker/src/osv/upsertAdvisory.ts) ----

export async function upsertAdvisory(
  qx: QueryExecutor,
  advisory: AdvisoryUpsertInput,
): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO advisories
      (osv_id, source, source_url, aliases, severity, cvss, cvss_source,
       summary, details, published_at, modified_at)
    VALUES
      ($(osvId), $(source), $(sourceUrl), $(aliases)::text[], $(severity),
       $(cvss), $(cvssSource), $(summary), $(details),
       $(publishedAt)::timestamptz, $(modifiedAt)::timestamptz)
    ON CONFLICT (osv_id) DO UPDATE SET
      source       = EXCLUDED.source,
      source_url   = EXCLUDED.source_url,
      aliases      = EXCLUDED.aliases,
      severity     = EXCLUDED.severity,
      cvss         = EXCLUDED.cvss,
      cvss_source  = EXCLUDED.cvss_source,
      summary      = EXCLUDED.summary,
      details      = EXCLUDED.details,
      published_at = EXCLUDED.published_at,
      modified_at  = EXCLUDED.modified_at
    RETURNING id
    `,
    advisory,
  )
  return row.id as number
}

// Resolves the packages.id for an OSV-referenced package, or null if the
// package isn't in our DB yet. Mirrors the COALESCE-aware unique index on
// packages(ecosystem, COALESCE(namespace,''), name).
export async function findPackageId(
  qx: QueryExecutor,
  pkg: PackageLookupInput,
): Promise<number | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT id
    FROM packages
    WHERE ecosystem = $(ecosystem)
      AND COALESCE(namespace, '') = COALESCE($(namespace), '')
      AND name = $(name)
    `,
    pkg,
  )
  return (row?.id as number | undefined) ?? null
}

export async function upsertAdvisoryPackage(
  qx: QueryExecutor,
  input: AdvisoryPackageUpsertInput,
): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO advisory_packages
      (advisory_id, package_id, ecosystem, package_name)
    VALUES
      ($(advisoryId), $(packageId), $(ecosystem), $(packageName))
    ON CONFLICT (advisory_id, ecosystem, package_name) DO UPDATE SET
      package_id = EXCLUDED.package_id
    RETURNING id
    `,
    input,
  )
  return row.id as number
}

// Only delete OSV-derived rows: rows with at least one of
// introduced/fixed/last_affected populated AND no deps.dev-source raw text
// columns. The deps.dev BQ worker (future) is expected to populate range_raw
// / unaffected_raw on rows of its own; we must not wipe those on every OSV
// pass.
export async function deleteOsvOnlyRanges(
  qx: QueryExecutor,
  advisoryPackageId: number,
): Promise<void> {
  await qx.result(
    `
    DELETE FROM advisory_affected_ranges
    WHERE advisory_package_id = $(advisoryPackageId)
      AND range_raw IS NULL
      AND unaffected_raw IS NULL
    `,
    { advisoryPackageId },
  )
}

export async function insertAdvisoryRange(
  qx: QueryExecutor,
  range: AdvisoryRangeInsertInput,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO advisory_affected_ranges
      (advisory_package_id, introduced_version, fixed_version, last_affected)
    VALUES
      ($(advisoryPackageId), $(introducedVersion), $(fixedVersion), $(lastAffected))
    `,
    range,
  )
}

// ---- derive path (services/apps/packages_worker/src/osv/deriveCriticalFlag.ts) ----

// Catch-up: resolve advisory_packages.package_id rows that were inserted
// before the package existed in our DB. Idempotent; cheap when there's
// nothing to do. Driven by the partial index
// `advisory_packages (ecosystem, package_name) WHERE package_id IS NULL`.
export async function resolveMissingPackageIds(qx: QueryExecutor): Promise<number> {
  return await qx.result(`
    UPDATE advisory_packages ap
    SET package_id = p.id
    FROM packages p
    WHERE ap.package_id IS NULL
      AND ap.ecosystem = p.ecosystem
      AND ap.package_name = CASE
        WHEN p.namespace IS NULL THEN p.name
        WHEN p.ecosystem = 'maven' THEN p.namespace || ':' || p.name
        WHEN p.ecosystem = 'npm' THEN '@' || p.namespace || '/' || p.name
        ELSE p.name
      END
  `)
}

export async function getPackagePage(
  qx: QueryExecutor,
  cursor: number,
  batchSize: number,
): Promise<PackageRow[]> {
  return qx.select(
    `
    SELECT id, ecosystem, latest_version
    FROM packages
    WHERE id > $(cursor) AND latest_version IS NOT NULL
    ORDER BY id
    LIMIT $(batchSize)
    `,
    { cursor, batchSize },
  )
}

// Returns every (range × advisory) row for the given package ids whose
// advisory is critical or a MAL- malicious-package report. Empty `ids`
// returns []; the caller is responsible for short-circuiting empty pages.
export async function getRangesForPackages(qx: QueryExecutor, ids: number[]): Promise<RangeRow[]> {
  if (ids.length === 0) return []
  return qx.select(
    `
    SELECT
      ap.package_id AS pkg_id,
      ar.introduced_version,
      ar.fixed_version,
      ar.last_affected,
      a.osv_id
    FROM advisory_packages ap
    JOIN advisories a ON a.id = ap.advisory_id
    JOIN advisory_affected_ranges ar ON ar.advisory_package_id = ap.id
    WHERE ap.package_id IN ($(ids:csv))
      AND (a.is_critical = TRUE OR a.osv_id LIKE 'MAL-%')
    `,
    { ids },
  )
}

export async function flipVulnerableFlags(qx: QueryExecutor, ids: number[]): Promise<number> {
  if (ids.length === 0) return 0
  return qx.result(
    `UPDATE packages SET has_critical_vulnerability = TRUE
     WHERE id IN ($(ids:csv)) AND has_critical_vulnerability = FALSE`,
    { ids },
  )
}

export async function clearSafeFlags(qx: QueryExecutor, ids: number[]): Promise<number> {
  if (ids.length === 0) return 0
  return qx.result(
    `UPDATE packages SET has_critical_vulnerability = FALSE
     WHERE id IN ($(ids:csv)) AND has_critical_vulnerability = TRUE`,
    { ids },
  )
}
