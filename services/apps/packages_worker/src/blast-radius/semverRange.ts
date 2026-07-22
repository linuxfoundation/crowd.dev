import * as semver from 'semver'

// Semver range utilities ported from the Python PoC (semver_utils.py).
// Handles version matching, range detection, and unparseable-range detection.

export interface SemverRange {
  introduced: string | null
  fixed: string | null
  lastAffected?: string | null
}

// Filter versions down to those satisfying all given semver ranges.
// A version must be in at least one range to be included.
export function versionsInRanges(versions: string[], ranges: SemverRange[]): string[] {
  const result: string[] = []

  for (const v of versions) {
    if (!semver.valid(v, { loose: true })) continue

    for (const range of ranges) {
      let rangeSpec = ''
      if (range.introduced && range.fixed) {
        rangeSpec = `>=${range.introduced} <${range.fixed}`
      } else if (range.introduced && range.lastAffected) {
        rangeSpec = `>=${range.introduced} <=${range.lastAffected}`
      } else if (range.introduced) {
        rangeSpec = `>=${range.introduced}`
      }

      if (rangeSpec && semver.satisfies(v, rangeSpec, { loose: true })) {
        result.push(v)
        break
      }
    }
  }

  return result
}

// Check whether a declared dependency range includes any of the vulnerable versions.
// Unparseable ranges (git URLs, tags, 'latest', workspace:* specs) conservatively
// return includes: true, check: 'unparseable-included'.
export function rangeIncludesAny(
  declaredRange: string,
  vulnerableVersions: string[],
): { includes: boolean; check: 'matched' | 'excluded' | 'unparseable-included' } {
  if (!declaredRange) {
    return { includes: true, check: 'unparseable-included' }
  }

  // Unparseable patterns: git URLs, file paths, tags, 'latest', workspace:* specs
  if (
    declaredRange.startsWith('git') ||
    declaredRange.startsWith('file:') ||
    declaredRange.startsWith('http://') ||
    declaredRange.startsWith('https://') ||
    declaredRange === 'latest' ||
    declaredRange.startsWith('workspace:') ||
    declaredRange.includes('tag:') ||
    !isValidSemverRange(declaredRange)
  ) {
    return { includes: true, check: 'unparseable-included' }
  }

  for (const vuln of vulnerableVersions) {
    if (semver.satisfies(vuln, declaredRange, { loose: true })) {
      return { includes: true, check: 'matched' }
    }
  }

  return { includes: false, check: 'excluded' }
}

// Get the highest version from a list, or null if none are valid.
export function highestVersion(versions: string[]): string | null {
  const valid = versions.filter((v) => semver.valid(v, { loose: true }))
  if (valid.length === 0) return null
  return semver.maxSatisfying(valid, '*', { loose: true }) || null
}

// Helper: check if a range spec is parseable as semver. semver.validRange returns null
// for unparseable specs rather than throwing, so the null check is required.
function isValidSemverRange(spec: string): boolean {
  try {
    return semver.validRange(spec, { loose: true }) !== null
  } catch {
    return false
  }
}
