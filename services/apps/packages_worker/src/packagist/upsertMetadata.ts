import {
  getPackagistPackageIdsByNames,
  reconcileVersionDependencies,
  updatePackagistVersionAggregates,
  upsertPackagistVersions,
} from '@crowd/data-access-layer/src/packages'
import type { VersionDependencyEdge } from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { stripNullBytesDeep } from '../utils/stripNullBytesDeep'

import {
  buildPackagistVersionRows,
  extractVersionDependencies,
  isPackagistDevVersion,
} from './normalize'
import type { PackagistDependency, PackagistExpandedVersion } from './types'

export async function persistPackagistMetadata(
  qx: QueryExecutor,
  purl: string,
  expanded: PackagistExpandedVersion[],
): Promise<{ found: boolean; changedFields: string[]; unresolvedDependencyTargets: number }> {
  // Registry data can contain NUL bytes (e.g. mojibake descriptions/licenses) that
  // Postgres text columns reject; strip them before any field is persisted.
  stripNullBytesDeep(expanded)

  const { versionRows, latestVersion, firstReleaseAt, latestReleaseAt, licenses, homepage } =
    buildPackagistVersionRows(expanded)

  let found = false
  const changedFields: string[] = []
  let unresolvedDependencyTargets = 0

  // One transaction for the whole p2 write path: the aggregates update, version
  // upsert + is_latest cleanup, and dependency reconcile (delete stale + upsert
  // current) are each multi-statement on their own — sharing one tx means a failure
  // partway through can never leave versions/dependencies half-refreshed.
  await qx.tx(async (t) => {
    const agg = await updatePackagistVersionAggregates(t, purl, {
      versionsCount: versionRows.length,
      latestVersion,
      firstReleaseAt,
      latestReleaseAt,
      licenses,
      homepage,
    })

    if (!agg) return

    found = true
    changedFields.push(...agg.changedFields)

    let versionIds: Array<{ number: string; id: string }> = []
    if (versionRows.length > 0) {
      const versionResult = await upsertPackagistVersions(t, agg.id, versionRows, latestVersion)
      changedFields.push(...versionResult.changedFields)
      versionIds = versionResult.versionIds
    }

    // One pass over the tagged versions collects target names and provisional edges;
    // target ids are then resolved in a single batch query.
    const versionMap = new Map(versionIds.map((v) => [v.number, v.id]))
    const targetNames = new Set<string>()
    const pending: Array<{ versionId: string; dep: PackagistDependency }> = []

    for (const v of expanded) {
      if (isPackagistDevVersion(v.version, v.version_normalized)) continue
      const versionId = versionMap.get(v.version)
      if (!versionId) continue

      for (const dep of extractVersionDependencies(v)) {
        targetNames.add(dep.name)
        pending.push({ versionId, dep })
      }
    }

    // Reconciled for every version being refreshed (not just ones with a resolved
    // dependency this pass) — a version whose manifest drops a requirement, or all of
    // them, must have its stale package_dependencies rows removed, not just the newly
    // declared ones upserted.
    if (versionIds.length > 0) {
      const edges: VersionDependencyEdge[] = []
      if (pending.length > 0) {
        const idMap = await getPackagistPackageIdsByNames(t, Array.from(targetNames))
        for (const { versionId, dep } of pending) {
          const dependsOnId = idMap.get(dep.name)
          if (!dependsOnId) {
            unresolvedDependencyTargets++
            continue
          }
          edges.push({
            packageId: agg.id,
            versionId,
            dependsOnId,
            constraint: dep.constraint,
            kind: dep.kind,
          })
        }
      }

      const depChanges = await reconcileVersionDependencies(
        t,
        versionIds.map((v) => v.id),
        edges,
      )
      changedFields.push(...depChanges)
    }
  })

  return { found, changedFields, unresolvedDependencyTargets }
}
