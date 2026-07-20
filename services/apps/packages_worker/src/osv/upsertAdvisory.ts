import {
  findPackageId,
  reconcileOsvRanges,
  supersedeDepsDevRanges,
  upsertAdvisory,
  upsertAdvisoryPackage,
} from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { NormalizedRange, NormalizedRecord } from './types'

// Drop duplicate ranges that would collide on the unique index over
// (advisory_package_id, COALESCE(introduced_version,''),
//  COALESCE(fixed_version,''), COALESCE(last_affected,'')).
// We key on the full range tuple so two ranges sharing an introduced_version
// but differing in fixed_version or last_affected both survive — per
// osv-plan §2 decision #1 ("one package has many version ranges; no
// denormalization") and ADR-0001 §`advisory_affected_ranges` uniqueness scope.
// OSV still occasionally emits redundant
// events for the exact same tuple, which the Set collapses to one row.
export function dedupeRanges(ranges: NormalizedRange[]): NormalizedRange[] {
  const seen = new Set<string>()
  const out: NormalizedRange[] = []
  for (const r of ranges) {
    const key = `${r.introducedVersion ?? ''}|${r.fixedVersion ?? ''}|${r.lastAffected ?? ''}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push(r)
  }
  return out
}

async function upsertOne(qx: QueryExecutor, record: NormalizedRecord): Promise<void> {
  const { advisory, packages } = record
  if (packages.length === 0) return

  const advisoryId = await upsertAdvisory(qx, advisory)

  for (const entry of packages) {
    const packageId = await findPackageId(qx, entry.pkg)

    const advisoryPackageId = await upsertAdvisoryPackage(qx, {
      advisoryId,
      packageId,
      ecosystem: entry.pkg.ecosystem,
      packageName: entry.pkg.packageName,
    })

    await reconcileOsvRanges(
      qx,
      advisoryPackageId,
      dedupeRanges(entry.ranges).map((range) => ({
        advisoryPackageId,
        introducedVersion: range.introducedVersion,
        fixedVersion: range.fixedVersion,
        lastAffected: range.lastAffected,
      })),
    )
    await supersedeDepsDevRanges(qx, advisoryPackageId)
  }
}

// upsertAdvisoryBatch writes a batch of normalized OSV records, one record per
// transaction. Per-record scope keeps advisory_packages row locks short (a few
// statements instead of ~OSV_BATCH_SIZE × N+1) so concurrent writers aren't
// blocked, and a Temporal activity cancel mid-batch only loses the in-flight
// record instead of forcing the whole batch to re-do. upsertOne is idempotent
// on osv_id, so a Temporal retry that re-runs the activity simply re-UPSERTs
// the already-committed records with the same values.
export async function upsertAdvisoryBatch(
  qx: QueryExecutor,
  batch: NormalizedRecord[],
): Promise<void> {
  for (const record of batch) {
    await qx.tx(async (tx) => {
      await upsertOne(tx, record)
    })
  }
}
