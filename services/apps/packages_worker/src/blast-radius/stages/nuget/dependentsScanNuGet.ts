import { getReverseDependents } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DependentCandidate, ScanDependentsResult } from '../../dependentsScan'
import { highestVersion } from '../ecosystemVersions'

import { nugetConstraintMayInclude } from './nugetConstraint'

// NuGet dependents come from package_dependencies (deps.dev BigQuery ingestion), same as
// Maven/Go — no download-count signal, and (unlike Maven) deps.dev never resolves a
// concrete version for NuGet edges, so matching goes purely through version_constraint.
export async function scanNuGetDependents(
  qx: QueryExecutor,
  vulnerablePackageId: string,
  vulnerableVersions: string[],
  topN: number,
): Promise<ScanDependentsResult> {
  const maxVulnerableVersion = highestVersion('nuget', vulnerableVersions)
  if (!maxVulnerableVersion) {
    return {
      source: 'package_dependencies',
      candidatesConsidered: 0,
      analyzed: [],
      excludedByRange: [],
      excludedByRangeCount: 0,
    }
  }

  // Cap distinct from topN: gather a wider pool so excludedByRange candidates are
  // still visible for diagnostics, same pattern as Maven/Go's scanLimit.
  const scanLimit = Math.max(topN * 8, 200)
  const rows = await getReverseDependents(qx, vulnerablePackageId, 'nuget', scanLimit)

  const candidates: DependentCandidate[] = rows.map((row) => {
    const rangeCheck = nugetConstraintMayInclude(row.versionConstraint, vulnerableVersions)
    return {
      name: row.name,
      version: row.versionNumber,
      downloads: row.dependentReposCount ?? row.dependentCount ?? null,
      declaredRange: row.versionConstraint,
      dependencyKind: row.dependencyKind,
      rangeIncludesVuln: rangeCheck !== 'excluded',
      rangeCheck,
      tarballUrl: null,
    }
  })

  const included = candidates.filter((c) => c.rangeIncludesVuln)
  const excluded = candidates.filter((c) => !c.rangeIncludesVuln)

  return {
    source: 'package_dependencies',
    candidatesConsidered: candidates.length,
    analyzed: included.slice(0, topN),
    excludedByRange: excluded.slice(0, 200),
    excludedByRangeCount: excluded.length,
  }
}
