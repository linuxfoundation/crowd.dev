import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { scanDependents } from '../dependentsScan'

export async function runDependentsStage(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
): Promise<void> {
  const startTime = Date.now()

  try {
    // Check if already done. Guard on the stage_run's own status rather than
    // candidates_considered — that column is set before completeStageRun, so a
    // crash between the two would otherwise make this stage look permanently done.
    const existingStatus = await blastRadiusDal.getStageRunStatus(qx, analysisId, 'dependents')
    if (existingStatus === 'succeeded') {
      return
    }

    // Start stage run record
    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'dependents',
      status: 'running',
      model: null,
    })

    // Get symbol spec from stage 1
    const spec = await blastRadiusDal.getSymbolSpec(qx, analysisId)
    if (!spec) {
      throw new Error('Symbol spec not found; stage 1 (intel) must run first')
    }

    const package_ = String(spec.package)
    const vulnerableVersions = (spec.vulnerable_versions || []) as string[]
    const relatedAffectedPackages = (spec.related_affected_packages || []) as string[]

    // Scan dependents
    const scanResult = await scanDependents({
      vulnerablePackage: package_,
      relatedAffectedPackages,
      vulnerableVersions,
      topN: 25,
      onProgress,
    })

    // Persist dependents
    const dependentInputs = [
      ...scanResult.analyzed.map((d) => ({
        analysisId,
        packageId: null,
        name: d.name,
        version: d.version,
        downloads: d.downloads,
        declaredRange: d.declaredRange,
        dependencyKind: d.dependencyKind,
        rangeIncludesVuln: d.rangeIncludesVuln,
        rangeCheck: d.rangeCheck,
        tarballUrl: d.tarballUrl,
        excludedByRange: false,
        exclusionReason: null,
      })),
      ...scanResult.excludedByRange.map((d) => ({
        analysisId,
        packageId: null,
        name: d.name,
        version: d.version,
        downloads: d.downloads,
        declaredRange: d.declaredRange,
        dependencyKind: d.dependencyKind,
        rangeIncludesVuln: d.rangeIncludesVuln,
        rangeCheck: d.rangeCheck,
        tarballUrl: d.tarballUrl,
        excludedByRange: true,
        exclusionReason: `Range does not include vulnerable versions (${d.rangeCheck})`,
      })),
    ]

    await blastRadiusDal.insertDependents(qx, dependentInputs)
    await blastRadiusDal.setDependentsMeta(
      qx,
      analysisId,
      scanResult.source,
      scanResult.candidatesConsidered,
    )

    const duration = Date.now() - startTime
    await blastRadiusDal.completeStageRun(qx, analysisId, 'dependents', duration, 0)
  } catch (err) {
    const duration = Date.now() - startTime
    const errorMsg = err instanceof Error ? err.message : String(err)
    await blastRadiusDal.failStageRun(qx, analysisId, 'dependents', duration, errorMsg)
    throw err
  }
}
