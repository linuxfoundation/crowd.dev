import {
  RangeRow,
  clearSafeFlags,
  flipVulnerableFlags,
  getPackagePage,
  getRangesForPackages,
  resolveMissingPackageIds,
} from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { compareVersion } from './versionCompare'

const log = getServiceChildLogger('osv-sync:derive')

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
    const pageRows = await getPackagePage(qx, cursor, batchSize)
    if (pageRows.length === 0) break

    const ids = pageRows.map((r) => r.id)
    const rangeRows = await getRangesForPackages(qx, ids)

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

    flipped += await flipVulnerableFlags(qx, vulnerable)
    cleared += await clearSafeFlags(qx, safe)

    cursor = pageRows[pageRows.length - 1].id
  }

  return { flipped, cleared }
}
