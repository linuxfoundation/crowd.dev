import { ApplicationFailure } from '@temporalio/activity'

// Rejects loudly instead of silently falling back to entries[0], which used to analyze
// the wrong (or only one of several) artifact while reporting the analysis as completed.
export interface SelectedAdvisoryEntry<T> {
  entry: T
  relatedAffectedPackages: string[]
}

export function selectAdvisoryEntry<T extends { package: { name: string } }>(
  entries: T[],
  requestedPackageName: string | null,
  matchesRequested: (entry: T) => boolean,
  advisoryOsvId: string,
): SelectedAdvisoryEntry<T> {
  // `!== null`, not truthiness — an empty string is still an explicit (if malformed)
  // request and must go through matching/rejection, not be treated as "none requested".
  if (requestedPackageName !== null) {
    const entry = entries.find(matchesRequested)
    if (!entry) {
      const affectedNames = entries.map((e) => e.package.name).join(', ')
      throw ApplicationFailure.nonRetryable(
        `Requested package ${requestedPackageName} not found in advisory ${advisoryOsvId} ` +
          `(affected: ${affectedNames})`,
        'ADVISORY_PACKAGE_NOT_FOUND',
      )
    }
    return {
      entry,
      relatedAffectedPackages: entries
        .map((e) => e.package.name)
        .filter((name) => name !== entry.package.name),
    }
  }

  if (entries.length > 1) {
    const affectedNames = entries.map((e) => e.package.name).join(', ')
    throw ApplicationFailure.nonRetryable(
      `Advisory ${advisoryOsvId} affects ${entries.length} packages (${affectedNames}); ` +
        `advisory-wide analysis is not supported for multi-artifact advisories — specify one via 'package'`,
      'ADVISORY_MULTI_ARTIFACT_AMBIGUOUS',
    )
  }

  return { entry: entries[0], relatedAffectedPackages: [] }
}
