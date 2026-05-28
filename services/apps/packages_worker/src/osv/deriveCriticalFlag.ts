import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { compareVersion } from './versionCompare'

const log = getServiceChildLogger('osv-sync:derive')

interface PackageRow {
  id: number
  ecosystem: string
  latest_version: string
}

interface RangeRow {
  pkg_id: number
  introduced_version: string | null
  fixed_version: string | null
  last_affected: string | null
  osv_id: string
}

// True iff `version` falls inside the OSV range, per the OSV schema semantics:
//   introduced=null|'0'  -> "from the beginning"
//   fixed=null           -> "no fix yet" (vulnerable forever from introduced)
//   last_affected=null   -> "no upper bound"
// We treat a comparator return of null (unparseable) as "no match" so a single
// odd version string never flips the flag without evidence.
function isInRange(ecosystem: string, version: string, range: RangeRow): boolean {
  const introduced = range.introduced_version
  if (introduced && introduced !== '0') {
    const c = compareVersion(ecosystem, version, introduced)
    if (c === null || c < 0) return false
  }

  if (range.fixed_version) {
    const c = compareVersion(ecosystem, version, range.fixed_version)
    if (c === null || c >= 0) return false
  }

  if (range.last_affected) {
    const c = compareVersion(ecosystem, version, range.last_affected)
    if (c === null || c > 0) return false
  }

  // MAL- ranges often have introduced=null/0 with both fixed and last_affected
  // null. That collapses to "always vulnerable" — the early returns above never
  // fire, so we fall through to true here.
  return true
}

// Catch-up: resolve advisory_packages.package_id rows that were inserted before
// the package existed in our DB. Idempotent; cheap when there's nothing to do.
async function resolveMissingPackageIds(qx: QueryExecutor): Promise<number> {
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

export async function deriveCriticalFlag(
  qx: QueryExecutor,
  batchSize: number,
): Promise<{ flipped: number; cleared: number }> {
  const resolved = await resolveMissingPackageIds(qx)
  if (resolved > 0) log.info({ resolved }, 'Resolved advisory_packages.package_id rows')

  let cursor = 0
  let flipped = 0
  let cleared = 0

  // Page through packages keyed by id. We compute the vulnerability decision in
  // TypeScript because the comparator is ecosystem-specific (semver / Maven) and
  // not expressible in SQL.
  /* eslint-disable no-constant-condition */
  while (true) {
    const pageRows: PackageRow[] = await qx.select(
      `
      SELECT id, ecosystem, latest_version
      FROM packages
      WHERE id > $(cursor) AND latest_version IS NOT NULL
      ORDER BY id
      LIMIT $(batchSize)
      `,
      { cursor, batchSize },
    )
    if (pageRows.length === 0) break

    const ids = pageRows.map((r) => r.id)
    const rangeRows: RangeRow[] = await qx.select(
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

    const rangesByPkg = new Map<number, RangeRow[]>()
    for (const r of rangeRows) {
      const list = rangesByPkg.get(r.pkg_id) ?? []
      list.push(r)
      rangesByPkg.set(r.pkg_id, list)
    }

    const vulnerable: number[] = []
    const safe: number[] = []
    for (const row of pageRows) {
      const ranges = rangesByPkg.get(row.id) ?? []
      const isVuln = ranges.some((rng) => isInRange(row.ecosystem, row.latest_version, rng))
      ;(isVuln ? vulnerable : safe).push(row.id)
    }

    if (vulnerable.length > 0) {
      flipped += await qx.result(
        `UPDATE packages SET has_critical_vulnerability = TRUE
         WHERE id IN ($(ids:csv)) AND has_critical_vulnerability = FALSE`,
        { ids: vulnerable },
      )
    }
    if (safe.length > 0) {
      cleared += await qx.result(
        `UPDATE packages SET has_critical_vulnerability = FALSE
         WHERE id IN ($(ids:csv)) AND has_critical_vulnerability = TRUE`,
        { ids: safe },
      )
    }

    cursor = pageRows[pageRows.length - 1].id
  }

  return { flipped, cleared }
}
