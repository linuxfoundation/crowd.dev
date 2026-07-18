import type {
  NormalizedPackagistStats,
  PackagistDependency,
  PackagistExpandedVersion,
  PackagistPackageInfo,
  PackagistVersionRow,
} from './types'

const PURL_COMPOSER_PREFIX = 'pkg:composer/'

export function packagistNameFromPurl(purl: string): string {
  return purl.slice(PURL_COMPOSER_PREFIX.length)
}

function blankToNull(s: string | null | undefined): string | null {
  if (s == null) return null
  const t = s.trim()
  return t || null
}

// versions.licenses is documented as a deterministically-sorted SPDX array (schema
// comment, V1779710880__initial_schema.sql) — Composer allows dual/multi-licensed
// releases, so unlike npm/pypi's near-always-single-element input, the same license
// set can arrive from the registry in a different order between fetches. Sorting once
// here keeps semantically identical arrays byte-identical, avoiding false audit noise.
function sortLicenses(licenses: string[] | null | undefined): string[] | null {
  return licenses && licenses.length > 0 ? [...licenses].sort() : null
}

export function normalizePackagistStats(pkg: PackagistPackageInfo): NormalizedPackagistStats {
  const description = blankToNull(pkg.description)
  const repositoryUrl = blankToNull(pkg.repository)

  // Determine status: abandoned → deprecated, else active
  let status: 'active' | 'deprecated' = 'active'
  if (pkg.abandoned === true || (typeof pkg.abandoned === 'string' && pkg.abandoned)) {
    status = 'deprecated'
  }

  // Extract downloads, defaulting to null if not a number. downloads.monthly is
  // deliberately not extracted here — packages.downloads_last_30d belongs exclusively
  // to the dedicated downloads-30d lane's boundary-anchored snapshot.
  const downloadsTotal = typeof pkg.downloads?.total === 'number' ? pkg.downloads.total : null

  const dependents = typeof pkg.dependents === 'number' ? pkg.dependents : null

  // Extract maintainers with non-empty names. isPackagistStatsJson only guarantees
  // maintainers is an array — a rogue null/non-object element would throw on `m.name`.
  const maintainers = (pkg.maintainers ?? [])
    .filter(
      (m): m is { name: string } =>
        typeof m === 'object' && m !== null && typeof m.name === 'string' && !!m.name,
    )
    .map((m) => ({
      username: m.name,
      displayName: null,
      email: null,
      role: 'maintainer' as const,
    }))

  return {
    name: pkg.name,
    description,
    repositoryUrl,
    status,
    dependents,
    downloadsTotal,
    maintainers,
  }
}

export function isPackagistDevVersion(version: string, versionNormalized?: string): boolean {
  if (version.startsWith('dev-')) return true
  if (versionNormalized?.endsWith('-dev')) return true
  return false
}

export function isPackagistPrerelease(versionNormalized: string): boolean {
  const parts = versionNormalized.split('-')
  if (parts.length < 2) return false

  const suffix = parts[1].toLowerCase()

  // Match: alpha|a|beta|b|rc followed by optional digits
  // Stable: plain number, -patch\d*, -p\d*
  if (suffix.match(/^(alpha|a|beta|b|rc)\d*$/)) {
    return true
  }

  return false
}

// Custom compare — the repo's semver/osv versionCompare can't be reused here.
// Composer's version_normalized is 4-part (e.g. 2.0.0.0); semver.parse returns null
// on it, and the osv wrapper deliberately refuses lossy coercion.
const PRERELEASE_RANKS: Record<string, number> = { alpha: 1, a: 1, beta: 2, b: 2, rc: 3 }
const STABLE_RANK = 4

function parseComposerVersion(v: string): { base: number[]; rank: number; suffixNum: number } {
  const dashIdx = v.indexOf('-')
  const base = (dashIdx === -1 ? v : v.slice(0, dashIdx)).split('.').map((p) => {
    const n = Number(p)
    return Number.isFinite(n) ? n : 0
  })

  const suffixMatch =
    dashIdx === -1 ? null : v.slice(dashIdx + 1).match(/^(alpha|a|beta|b|rc)(\d*)/i)
  if (!suffixMatch) return { base, rank: STABLE_RANK, suffixNum: 0 }

  return {
    base,
    rank: PRERELEASE_RANKS[suffixMatch[1].toLowerCase()],
    suffixNum: suffixMatch[2] ? Number(suffixMatch[2]) : 0,
  }
}

function compareComposerVersions(a: string, b: string): number {
  const pa = parseComposerVersion(a)
  const pb = parseComposerVersion(b)

  const maxLen = Math.max(pa.base.length, pb.base.length)
  for (let i = 0; i < maxLen; i++) {
    const diff = (pa.base[i] ?? 0) - (pb.base[i] ?? 0)
    if (diff !== 0) return diff
  }

  if (pa.rank !== pb.rank) return pa.rank - pb.rank
  return pa.suffixNum - pb.suffixNum
}

export function buildPackagistVersionRows(versions: PackagistExpandedVersion[]): {
  versionRows: PackagistVersionRow[]
  latestVersion: string | null
  firstReleaseAt: string | null
  latestReleaseAt: string | null
  licenses: string[] | null
  homepage: string | null
} {
  // Filter out dev versions
  const kept = versions.filter((v) => !isPackagistDevVersion(v.version, v.version_normalized))

  if (kept.length === 0) {
    return {
      versionRows: [],
      latestVersion: null,
      firstReleaseAt: null,
      latestReleaseAt: null,
      licenses: null,
      homepage: null,
    }
  }

  const versionRows: PackagistVersionRow[] = kept.map((v) => ({
    number: v.version,
    publishedAt: v.time ?? null,
    isLatest: false, // Will be set below
    isPrerelease: isPackagistPrerelease(v.version_normalized ?? v.version),
    licenses: sortLicenses(v.license),
  }))

  // Find latest: prefer stable over prerelease; within each group use Composer ordering
  let latestIdx = 0

  for (let i = 1; i < kept.length; i++) {
    if (versionRows[i].isPrerelease !== versionRows[latestIdx].isPrerelease) {
      // A stable release always beats a prerelease
      if (!versionRows[i].isPrerelease) latestIdx = i
      continue
    }

    const currentNorm = kept[i].version_normalized ?? kept[i].version
    const latestNorm = kept[latestIdx].version_normalized ?? kept[latestIdx].version
    if (compareComposerVersions(currentNorm, latestNorm) > 0) latestIdx = i
  }

  versionRows[latestIdx].isLatest = true

  // Compute aggregates
  const times = kept
    .map((v) => v.time)
    .filter((t): t is string => t != null)
    .sort()

  const firstReleaseAt = times[0] ?? null
  const latestReleaseAt = times[times.length - 1] ?? null

  // Licenses and homepage come from the latest row
  const licenses = sortLicenses(kept[latestIdx].license)
  const homepage = blankToNull(kept[latestIdx].homepage)

  return {
    versionRows,
    latestVersion: kept[latestIdx].version,
    firstReleaseAt,
    latestReleaseAt,
    licenses,
    homepage,
  }
}

export function extractVersionDependencies(
  version: PackagistExpandedVersion,
): PackagistDependency[] {
  const deps: PackagistDependency[] = []

  // Add require (direct) dependencies, excluding platform targets
  if (version.require && typeof version.require === 'object') {
    for (const [name, constraint] of Object.entries(version.require)) {
      if (typeof constraint === 'string' && name.includes('/')) {
        deps.push({ name, constraint, kind: 'direct' })
      }
    }
  }

  // Add require-dev (dev) dependencies, excluding platform targets
  if (version['require-dev'] && typeof version['require-dev'] === 'object') {
    for (const [name, constraint] of Object.entries(version['require-dev'])) {
      if (typeof constraint === 'string' && name.includes('/')) {
        deps.push({ name, constraint, kind: 'dev' })
      }
    }
  }

  return deps
}
