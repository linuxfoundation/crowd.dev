import {
  getPackagistPackageIdsByNames,
  updatePackagistVersionAggregates,
  upsertPackagistVersions,
  upsertVersionDependencies,
} from '@crowd/data-access-layer/src/packages'
import type { VersionDependencyEdge } from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

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
  const { versionRows, latestVersion, firstReleaseAt, latestReleaseAt, licenses, homepage } =
    buildPackagistVersionRows(expanded)

  const agg = await updatePackagistVersionAggregates(qx, purl, {
    versionsCount: versionRows.length,
    latestVersion,
    firstReleaseAt,
    latestReleaseAt,
    licenses,
    homepage,
  })

  if (!agg) {
    return { found: false, changedFields: [], unresolvedDependencyTargets: 0 }
  }

  const changedFields = [...agg.changedFields]

  let versionIds: Array<{ number: string; id: string }> = []
  if (versionRows.length > 0) {
    const versionResult = await upsertPackagistVersions(qx, agg.id, versionRows, latestVersion)
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

  let unresolvedDependencyTargets = 0
  if (pending.length > 0) {
    const idMap = await getPackagistPackageIdsByNames(qx, Array.from(targetNames))

    const edges: VersionDependencyEdge[] = []
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

    if (edges.length > 0) {
      await upsertVersionDependencies(qx, edges)
    }
  }

  return { found: true, changedFields, unresolvedDependencyTargets }
}
