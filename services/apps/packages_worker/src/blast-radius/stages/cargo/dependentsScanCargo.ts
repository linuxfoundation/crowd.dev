import { getReverseDependents } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DependentCandidate, ScanDependentsResult } from '../../dependentsScan'

import { cargoDependencyMayIncludeVuln } from './cargoConstraint'

// Cargo dependents come straight from our own DB (package_dependencies, populated via
// deps.dev BigQuery ingestion) rather than an external ranking list — no download-count
// signal exists for crates.io the way npm has one, same as Go.
export async function scanCargoDependents(
  qx: QueryExecutor,
  vulnerablePackageId: string,
  vulnerableVersions: string[],
  topN: number,
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
  // still visible for diagnostics, same pattern as npm's scanLimit.
  const scanLimit = Math.max(topN * 8, 200)
  const rows = await getReverseDependents(qx, vulnerablePackageId, 'cargo', scanLimit)

  const candidates: DependentCandidate[] = rows.map((row) => {
    const rangeCheck = cargoDependencyMayIncludeVuln(
      row.versionNumber,
      row.versionConstraint,
      vulnerableVersions,
    )
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
