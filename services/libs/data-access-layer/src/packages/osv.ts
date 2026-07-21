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
       summary, details, published_at, modified_at, created_at, updated_at)
    VALUES
      ($(osvId), $(source), $(sourceUrl), $(aliases)::text[], $(severity),
       $(cvss), $(cvssSource), $(summary), $(details),
       $(publishedAt)::timestamptz, $(modifiedAt)::timestamptz, NOW(), NOW())
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
      modified_at  = EXCLUDED.modified_at,
      updated_at   = EXCLUDED.updated_at
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
      (advisory_id, package_id, ecosystem, package_name, created_at, updated_at)
    VALUES
      ($(advisoryId), $(packageId), $(ecosystem), $(packageName), NOW(), NOW())
    ON CONFLICT (advisory_id, ecosystem, package_name) DO UPDATE SET
      package_id = EXCLUDED.package_id,
      updated_at = EXCLUDED.updated_at
    RETURNING id
    `,
    input,
  )
  return row.id as number
}

// advisory_packages rows OSV previously wrote for this advisory that are absent
// from the current payload. parseOsvRecord drops a package once it has no
// usable ranges left, and a corrected upstream record can drop a package
// outright — either way the package never reaches upsertOne's per-entry loop,
// so its previously live OSV ranges would sit as permanent false positives and
// block deps.dev's NOT EXISTS ownership guard forever unless reconciled as a
// removal (see upsertOne in services/apps/packages_worker/src/osv/upsertAdvisory.ts).
export async function findRemovedAdvisoryPackageIds(
  qx: QueryExecutor,
  advisoryId: number,
  presentAdvisoryPackageIds: number[],
): Promise<number[]> {
  const rows = await qx.select(
    `
    SELECT id
    FROM advisory_packages
    WHERE advisory_id = $(advisoryId)
      AND NOT (id = ANY($(presentAdvisoryPackageIds)::bigint[]))
    `,
    { advisoryId, presentAdvisoryPackageIds },
  )
  return rows.map((r: { id: number }) => r.id)
}

// Diff-based upsert + soft-delete for OSV-owned advisory_affected_ranges rows.
// Replaces the old hard-delete + reinsert sweep (which minted a new PK for
// every unchanged range every sync, producing zombie rows once Sequin
// replicates DELETEs into a Tinybird ReplacingMergeTree that has no
// sign/is_deleted column — see ADR-0001 §`advisory_affected_ranges`
// delete/dedup strategy, CM-1258).
//
// An advisory_package whose range set hasn't changed since the last sync now
// produces zero writes: matching tuples are left untouched (not even an
// `updated_at` bump), so no Sequin/Tinybird event fires for them either.
//
// Tuple in both old and new, already a clean live OSV row -> untouched.
// Tuple only in new, or matching a live deps.dev raw row on the same key ->
// upserted (INSERT ON CONFLICT; clears deleted_at in case it was previously
// soft-deleted, and clears range_raw/unaffected_raw to reclaim the row from
// deps.dev — OSV wins on key overlap per ADR-0001 §Write semantics). Tuple
// only in old -> soft-deleted.
// Soft-delete (not hard-delete) is required because the unique key is the
// value tuple itself: when OSV corrects a range, the corrected tuple has no
// successor row to collapse into, so without deleted_at the stale tuple
// would sit forever as a false-positive vulnerable-range match.
export async function reconcileOsvRanges(
  qx: QueryExecutor,
  advisoryPackageId: number,
  ranges: AdvisoryRangeInsertInput[],
): Promise<void> {
  const values = ranges.map((r) => ({
    introduced_version: r.introducedVersion,
    fixed_version: r.fixedVersion,
    last_affected: r.lastAffected,
  }))

  await qx.result(
    `
    INSERT INTO advisory_affected_ranges
      (advisory_package_id, introduced_version, fixed_version, last_affected, created_at, updated_at)
    SELECT $(advisoryPackageId), v.introduced_version, v.fixed_version, v.last_affected, NOW(), NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      introduced_version text,
      fixed_version text,
      last_affected text
    )
    ON CONFLICT (advisory_package_id, COALESCE(introduced_version, ''), COALESCE(fixed_version, ''), COALESCE(last_affected, ''))
    DO UPDATE SET
      updated_at = NOW(),
      deleted_at = NULL,
      range_raw = NULL,
      unaffected_raw = NULL
    WHERE advisory_affected_ranges.deleted_at IS NOT NULL
       OR advisory_affected_ranges.range_raw IS NOT NULL
       OR advisory_affected_ranges.unaffected_raw IS NOT NULL
    `,
    { advisoryPackageId, values: JSON.stringify(values) },
  )

  // Soft-delete OSV-owned live rows whose tuple isn't in the new set anymore.
  await qx.result(
    `
    UPDATE advisory_affected_ranges ar
    SET deleted_at = NOW(),
        updated_at = NOW()
    WHERE ar.advisory_package_id = $(advisoryPackageId)
      AND ar.deleted_at IS NULL
      AND ar.range_raw IS NULL
      AND ar.unaffected_raw IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM jsonb_to_recordset($(values)::jsonb) AS v(
          introduced_version text,
          fixed_version text,
          last_affected text
        )
        WHERE COALESCE(v.introduced_version, '') = COALESCE(ar.introduced_version, '')
          AND COALESCE(v.fixed_version, '') = COALESCE(ar.fixed_version, '')
          AND COALESCE(v.last_affected, '') = COALESCE(ar.last_affected, '')
      )
    `,
    { advisoryPackageId, values: JSON.stringify(values) },
  )
}

// OSV is the source of truth for ranges over time (see ADR-0001 §Write
// semantics `advisories` row): once OSV has written its structured ranges
// for an advisory_package, any live deps.dev raw row for that same
// advisory_package is superseded and should stop being treated as current.
// Soft-deleted (not hard-deleted) for the same reason as reconcileOsvRanges —
// no successor row exists to collapse a deps.dev tuple into.
export async function supersedeDepsDevRanges(
  qx: QueryExecutor,
  advisoryPackageId: number,
): Promise<void> {
  await qx.result(
    `
    UPDATE advisory_affected_ranges
    SET deleted_at = NOW(),
        updated_at = NOW()
    WHERE advisory_package_id = $(advisoryPackageId)
      AND deleted_at IS NULL
      AND (range_raw IS NOT NULL OR unaffected_raw IS NOT NULL)
    `,
    { advisoryPackageId },
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
    JOIN advisory_affected_ranges ar ON ar.advisory_package_id = ap.id AND ar.deleted_at IS NULL
    WHERE ap.package_id IN ($(ids:csv))
      AND (a.is_critical = TRUE OR a.osv_id LIKE 'MAL-%')
    `,
    { ids },
  )
}

export async function flipVulnerableFlags(qx: QueryExecutor, ids: number[]): Promise<number> {
  if (ids.length === 0) return 0
  return qx.result(
    `UPDATE packages SET has_critical_vulnerability = TRUE, last_synced_at = NOW()
     WHERE id IN ($(ids:csv)) AND has_critical_vulnerability = FALSE`,
    { ids },
  )
}

export async function clearSafeFlags(qx: QueryExecutor, ids: number[]): Promise<number> {
  if (ids.length === 0) return 0
  return qx.result(
    `UPDATE packages SET has_critical_vulnerability = FALSE, last_synced_at = NOW()
     WHERE id IN ($(ids:csv)) AND has_critical_vulnerability = TRUE`,
    { ids },
  )
}
