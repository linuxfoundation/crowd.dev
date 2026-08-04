import { getReverseDependents } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DependentCandidate, ScanDependentsResult } from '../../dependentsScan'
import { highestVersion } from '../../semverRange'

import { goConstraintMayInclude } from './goConstraint'

// Go dependents come straight from our own DB (package_dependencies, populated via
// deps.dev BigQuery ingestion) rather than an external ranking list like npm's
// npm-high-impact package — no download-count signal exists for Go, so ranking uses
// dependent_repos_count / dependent_count (see getReverseDependents' ORDER BY).
export async function scanGoDependents(
  qx: QueryExecutor,
  vulnerablePackageId: string,
  vulnerableVersions: string[],
  topN: number,
): Promise<ScanDependentsResult> {
  const maxVulnerableVersion = highestVersion(vulnerableVersions)
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
  // still visible for diagnostics, same pattern as npm's scanLimit.
  const scanLimit = Math.max(topN * 8, 200)
  const rows = await getReverseDependents(qx, vulnerablePackageId, 'go', scanLimit)

  const candidates: DependentCandidate[] = rows.map((row) => {
    const rangeCheck = goConstraintMayInclude(row.versionConstraint, maxVulnerableVersion)
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
