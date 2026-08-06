import { getReverseDependents } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DependentCandidate, ScanDependentsResult } from '../../dependentsScan'

import { rubygemsConstraintMayInclude } from './rubygemsConstraint'

// Same as Maven/Go/NuGet: no download-count signal, and deps.dev never resolves a
// concrete version for RubyGems edges, so matching goes purely through version_constraint.
export async function scanRubyGemsDependents(
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
  // still visible for diagnostics, same pattern as Maven/Go/NuGet's scanLimit.
  const scanLimit = Math.max(topN * 8, 200)
  const rows = await getReverseDependents(qx, vulnerablePackageId, 'rubygems', scanLimit)

  const candidates: DependentCandidate[] = rows.map((row) => {
    const rangeCheck = rubygemsConstraintMayInclude(row.versionConstraint, vulnerableVersions)
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
