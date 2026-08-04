// Rejects loudly instead of silently falling back to entries[0], which used to analyze
// the wrong (or only one of several) artifact while reporting the analysis as completed.
export function selectAdvisoryEntry<T extends { package: { name: string } }>(
  entries: T[],
  requestedPackageName: string | null,
  matchesRequested: (entry: T) => boolean,
  advisoryOsvId: string,
): T {
  const affectedNames = entries.map((e) => e.package.name)

  if (requestedPackageName) {
    const entry = entries.find(matchesRequested)
    if (!entry) {
      throw new Error(
        `Requested package ${requestedPackageName} not found in advisory ${advisoryOsvId} ` +
          `(affected: ${affectedNames.join(', ')})`,
      )
    }
    return entry
  }

  if (entries.length > 1) {
    throw new Error(
      `Advisory ${advisoryOsvId} affects ${entries.length} packages (${affectedNames.join(', ')}); ` +
        `advisory-wide analysis is not supported for multi-artifact advisories — specify one via 'package'`,
    )
  }

  return entries[0]
}
