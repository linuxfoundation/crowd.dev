import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { findPackageIdsByName } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { scanPyPiDependents } from './dependentsScanPyPi'

export async function runDependentsStagePyPi(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
  signal?: AbortSignal,
): Promise<void> {
  const startTime = Date.now()

  try {
    const existingStatus = await blastRadiusDal.getStageRunStatus(qx, analysisId, 'dependents')
    if (existingStatus === 'succeeded') {
      return
    }

    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'dependents',
      status: 'running',
      model: null,
    })

    const spec = await blastRadiusDal.getSymbolSpec(qx, analysisId)
    if (!spec) {
      throw new Error('Symbol spec not found; stage 1 (intel) must run first')
    }

    // See dependentsNpm.ts for why this unconditional clear is safe: reachability
    // hasn't produced any verdicts yet at this point in the pipeline.
    await blastRadiusDal.deleteDependents(qx, analysisId)

    const analysis = await blastRadiusDal.getAnalysis(qx, analysisId)
    if (!analysis?.package_id) {
      throw new Error('Vulnerable project package_id not resolved; stage 1 (intel) must run first')
    }

    const vulnerableVersions = (spec.vulnerable_versions || []) as string[]
    const relatedAffectedPackages = (spec.related_affected_packages || []) as string[]

    onProgress?.()

    const scanResult = await scanPyPiDependents(
      qx,
      String(analysis.package_id),
      vulnerableVersions,
      25,
      relatedAffectedPackages,
    )

    if (signal?.aborted) {
      throw new Error('Dependents scan cancelled')
    }
    onProgress?.()

    // scanResult.analyzed's names come straight from packages.name via getReverseDependents,
    // so they round-trip consistently — no purl-normalization dance needed here.
    const packageIdsByName = await findPackageIdsByName(
      qx,
      'pypi',
      scanResult.analyzed.map((d) => d.name),
    )

    const dependentInputs = [
      ...scanResult.analyzed.map((d) => ({
        analysisId,
        packageId: packageIdsByName.get(d.name) ?? null,
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
        exclusionReason: `Requirement does not include vulnerable versions (${d.rangeCheck})`,
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
