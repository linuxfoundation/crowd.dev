import {
  AdvisoryRangeInsertInput,
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

  const entries: { advisoryPackageId: number; ranges: AdvisoryRangeInsertInput[] }[] = []
  for (const entry of packages) {
    const packageId = await findPackageId(qx, entry.pkg)

    const advisoryPackageId = await upsertAdvisoryPackage(qx, {
      advisoryId,
      packageId,
      ecosystem: entry.pkg.ecosystem,
      packageName: entry.pkg.packageName,
    })

    entries.push({
      advisoryPackageId,
      ranges: dedupeRanges(entry.ranges).map((range) => ({
        advisoryPackageId,
        introducedVersion: range.introducedVersion,
        fixedVersion: range.fixedVersion,
        lastAffected: range.lastAffected,
      })),
    })
  }

  // Lock advisory_packages rows in ascending id order — the same order deps.dev's
  // bulk merge locks them in (ADVISORY_PACKAGES_LOCK_SQL's ORDER BY ap.id in
  // ingestAdvisories.ts). OSV's payload order is otherwise arbitrary; locking out
  // of order here would let this per-record transaction and a concurrent deps.dev
  // chunk each hold one row and wait on the other's, deadlocking — and deps.dev's
  // maximumAttempts: 1 turns a deadlock abort into a hard merge failure, not a retry.
  entries.sort((a, b) => a.advisoryPackageId - b.advisoryPackageId)

  for (const { advisoryPackageId, ranges } of entries) {
    // Row-locks this advisory_packages row (held until the enclosing transaction
    // commits) before touching advisory_affected_ranges, matching the lock
    // deps.dev's bulk merge takes on the same row — whichever writer locks first
    // forces the other to wait and see its committed writes, closing the
    // ownership race between the two independently-scheduled write paths
    // (ADR-0001 §Write semantics).
    await qx.result(`SELECT id FROM advisory_packages WHERE id = $(advisoryPackageId) FOR UPDATE`, {
      advisoryPackageId,
    })

    await reconcileOsvRanges(qx, advisoryPackageId, ranges)
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
