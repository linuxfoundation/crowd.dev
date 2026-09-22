// node-semver rejects any version with a 4th numeric component (e.g. "1.2.3.4"), but
// NuGetVersion accepts Major.Minor.Patch.Revision — common in Microsoft/BCL packages
// (System.*, Microsoft.NETCore.*). Routing NuGet through the shared semver comparator
// silently drops those versions from range checks; this is a NuGetVersion-compatible
// comparator scoped to blast-radius so it doesn't change the shared OSV sync pipeline.
interface ParsedNuGetVersion {
  numbers: [number, number, number, number]
  prerelease: string | null
}

function parseNuGetVersion(input: string): ParsedNuGetVersion | null {
  // NuGetVersion ignores build metadata (+xxx) for comparison purposes.
  const withoutMetadata = input.trim().split('+')[0]
  const dashIndex = withoutMetadata.indexOf('-')
  const versionPart = dashIndex === -1 ? withoutMetadata : withoutMetadata.slice(0, dashIndex)
  const prerelease = dashIndex === -1 ? null : withoutMetadata.slice(dashIndex + 1)
  if (prerelease === '') return null

  const segments = versionPart.split('.')
  if (segments.length === 0 || segments.length > 4) return null

  const numbers: [number, number, number, number] = [0, 0, 0, 0]
  for (let i = 0; i < segments.length; i++) {
    if (!/^\d+$/.test(segments[i])) return null
    numbers[i] = parseInt(segments[i], 10)
  }
  return { numbers, prerelease }
}

// NuGet prerelease identifiers compare dot-segment by dot-segment: numeric segments
// compare numerically, non-numeric compare ordinally, numeric sorts below non-numeric,
// and a shorter identifier list sorts below a longer one that shares its prefix.
function comparePrerelease(a: string, b: string): number {
  const aParts = a.split('.')
  const bParts = b.split('.')
  const len = Math.max(aParts.length, bParts.length)

  for (let i = 0; i < len; i++) {
    if (i >= aParts.length) return -1
    if (i >= bParts.length) return 1

    const aIsNum = /^\d+$/.test(aParts[i])
    const bIsNum = /^\d+$/.test(bParts[i])
    if (aIsNum && bIsNum) {
      const an = parseInt(aParts[i], 10)
      const bn = parseInt(bParts[i], 10)
      if (an !== bn) return an < bn ? -1 : 1
      continue
    }
    if (aIsNum !== bIsNum) return aIsNum ? -1 : 1
    if (aParts[i] !== bParts[i]) return aParts[i] < bParts[i] ? -1 : 1
  }
  return 0
}

export function compareNuGetVersion(a: string, b: string): number | null {
  const pa = parseNuGetVersion(a)
  const pb = parseNuGetVersion(b)
  if (!pa || !pb) return null

  for (let i = 0; i < 4; i++) {
    if (pa.numbers[i] !== pb.numbers[i]) return pa.numbers[i] < pb.numbers[i] ? -1 : 1
  }

  if (pa.prerelease === null && pb.prerelease === null) return 0
  if (pa.prerelease === null) return 1 // a release outranks any prerelease of the same numbers
  if (pb.prerelease === null) return -1
  return comparePrerelease(pa.prerelease, pb.prerelease)
}
