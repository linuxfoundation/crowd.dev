import { getReverseDependents } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DependentCandidate, ScanDependentsResult } from '../../dependentsScan'
import { toPypiNormalizedName } from '../../packageIdentifier'

import { pypiDependencyMayIncludeVuln } from './pypiConstraint'

// PyPI is a deps.dev EDGE ecosystem, same as npm/Maven/Cargo — dependents come from our
// own DB (package_dependencies), with a resolved version preferred as ground truth.
export async function scanPyPiDependents(
  qx: QueryExecutor,
  vulnerablePackageId: string,
  vulnerableVersions: string[],
  topN: number,
  relatedAffectedPackages?: string[],
): Promise<ScanDependentsResult> {
  if (vulnerableVersions.length === 0) {
    return {
      source: 'package_dependencies',
      candidatesConsidered: 0,
      analyzed: [],
      excludedByRange: [],
      excludedByRangeCount: 0,
    }
  }

  // Cap distinct from topN: gather a wider pool so excludedByRange candidates are
  // still visible for diagnostics, same pattern as npm's/cargo's scanLimit.
  const scanLimit = Math.max(topN * 8, 200)
  const rows = await getReverseDependents(qx, vulnerablePackageId, 'pypi', scanLimit)
  const relatedPackageNames = new Set((relatedAffectedPackages || []).map(toPypiNormalizedName))

  const included: DependentCandidate[] = []
  const excluded: DependentCandidate[] = []

  for (const row of rows) {
    if (relatedPackageNames.has(toPypiNormalizedName(row.name))) continue

    const rangeCheck = pypiDependencyMayIncludeVuln(
      row.resolvedVersionNumber,
      row.versionConstraint,
      vulnerableVersions,
    )
    const candidate: DependentCandidate = {
      name: row.name,
      version: row.versionNumber,
      downloads: row.dependentReposCount ?? row.dependentCount ?? null,
      declaredRange: row.versionConstraint,
      dependencyKind: row.dependencyKind,
      rangeIncludesVuln: rangeCheck !== 'excluded',
      rangeCheck,
      tarballUrl: null,
    }
    if (candidate.rangeIncludesVuln) {
      included.push(candidate)
    } else {
      excluded.push(candidate)
    }
  }

  return {
    source: 'package_dependencies',
    candidatesConsidered: included.length + excluded.length,
    analyzed: included.slice(0, topN),
    excludedByRange: excluded.slice(0, 200),
    excludedByRangeCount: excluded.length,
  }
}
