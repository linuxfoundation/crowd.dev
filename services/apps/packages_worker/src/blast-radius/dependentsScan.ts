import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import { fetchBulkPointRange, fetchPointRange } from '../npm/fetchDownloads'
import { fetchPackument } from '../npm/fetchPackument'
import { isFetchError } from '../npm/types'

import { fetchAbbreviatedPackument } from './clients/npmAbbreviated'
import { downloadAndExtractTarball } from './clients/npmTarball'
import { NpmVersionManifest, asNpmVersionManifest } from './npmManifest'
import { rangeIncludesAny } from './semverRange'

const SCAN_CONCURRENCY = 32
const HIGH_IMPACT_CACHE_TTL_MS = 24 * 60 * 60 * 1000
const DEP_KINDS: Array<'dependencies' | 'peerDependencies' | 'optionalDependencies'> = [
  'dependencies',
  'peerDependencies',
  'optionalDependencies',
]

let highImpactNamesCache: { names: string[]; fetchedAt: number } | null = null

// Runs `fn` over `items` with at most `concurrency` in flight at once.
async function mapWithConcurrency<T>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let next = 0
  async function worker() {
    while (next < items.length) {
      const i = next++
      await fn(items[i], i)
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker))
}

export interface DependentCandidate {
  name: string
  version: string | null
  downloads: number | null
  declaredRange: string | null
  dependencyKind: string | null
  rangeIncludesVuln: boolean
  rangeCheck: 'matched' | 'excluded' | 'unparseable-included'
  tarballUrl: string | null
}

export interface ScanDependentsResult {
  source: string
  candidatesConsidered: number
  analyzed: DependentCandidate[]
  excludedByRange: DependentCandidate[]
  excludedByRangeCount: number
}

// Fetch high-impact npm package names from the npm-high-impact package. Cached in-memory
// for the life of the worker process — the list is a published npm package that changes
// rarely, so re-downloading and re-extracting its tarball on every analysis is wasted work.
async function highImpactNames(): Promise<string[]> {
  if (
    highImpactNamesCache &&
    Date.now() - highImpactNamesCache.fetchedAt < HIGH_IMPACT_CACHE_TTL_MS
  ) {
    return highImpactNamesCache.names
  }

  const names = await fetchHighImpactNames()
  highImpactNamesCache = { names, fetchedAt: Date.now() }
  return names
}

async function fetchHighImpactNames(): Promise<string[]> {
  const packument = await fetchPackument('npm-high-impact')
  if (isFetchError(packument)) {
    throw new Error(`Failed to fetch npm-high-impact: ${packument.message}`)
  }

  const latest = packument['dist-tags']?.latest
  if (!latest) {
    throw new Error('npm-high-impact has no latest version')
  }

  const versionData = packument.versions?.[latest]
  if (!versionData) {
    throw new Error(`npm-high-impact version ${latest} not found`)
  }

  const tarballUrl = asNpmVersionManifest(versionData).dist?.tarball
  if (!tarballUrl) {
    throw new Error(`npm-high-impact ${latest} has no tarball`)
  }

  // Download and extract to temp directory
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'npm-high-impact-'))

  try {
    await downloadAndExtractTarball(tarballUrl, tempDir)

    // Parse lib files for package names
    const names = new Set<string>()
    const files = ['lib/top-download.js', 'lib/top-dependent.js', 'lib/top.js']

    for (const file of files) {
      const filePath = path.join(tempDir, file)
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf-8')
        for (const match of content.matchAll(/'((?:@[\w.-]+\/)?[\w.-]+)'/g)) {
          names.add(match[1])
        }
      }
    }

    return Array.from(names)
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true })
  }
}

// npm aliases ("myLodash": "npm:lodash@^4.17.21") install and execute the aliased
// package under a local name that has nothing to do with the target — a plain
// key lookup misses these entirely. Parses the "npm:<name>[@<range>]" spec value;
// the version-strip mirrors toBareNpmName's since npm scopes' own @ is always
// followed by /, so a trailing @range (no / or @) is never a scope separator.
function parseNpmAlias(spec: string): { name: string; range: string } | null {
  if (!spec.startsWith('npm:')) return null
  const rest = spec.slice('npm:'.length)
  const versionMatch = rest.match(/@([^/@]+)$/)
  return {
    name: versionMatch ? rest.slice(0, -versionMatch[0].length) : rest,
    range: versionMatch ? versionMatch[1] : '*',
  }
}

// Resolves a dependency's real target name and version range, unwrapping an
// npm: alias if present. Shared by declaredDependency and candidateNamesFromScan
// so alias-matching semantics can't drift between the two call sites.
function resolvedDependencyTarget(
  depName: string,
  depSpec: string,
): { name: string; range: string } {
  const alias = parseNpmAlias(depSpec)
  return alias ?? { name: depName, range: depSpec }
}

// Check if a package version declares a dependency on a target package,
// directly or via an npm: alias.
function declaredDependency(
  versionData: NpmVersionManifest,
  targetPackage: string,
  relatedPackages: string[],
): { kind: string; range: string } | null {
  const allTargets = [targetPackage, ...relatedPackages]

  for (const depKind of DEP_KINDS) {
    const deps = versionData[depKind] ?? {}
    for (const [depName, depSpec] of Object.entries(deps)) {
      const resolved = resolvedDependencyTarget(depName, depSpec)
      if (allTargets.includes(resolved.name)) {
        return { kind: depKind, range: resolved.range }
      }
    }
  }

  return null
}

// Concurrent, lightweight pre-scan: for each candidate name, fetch the abbreviated packument
// (dependency maps only, no readme/maintainers/etc.) and keep it only if its latest version
// declares a dependency on one of the targets. This mirrors the PoC's two-phase design — do the
// expensive full-packument fetch (below, in the ranking walk) only for names that actually
// matter, instead of one full packument per high-impact name.
async function candidateNamesFromScan(
  names: string[],
  targets: Set<string>,
  onProgress?: () => void,
): Promise<string[]> {
  const hits: string[] = []
  let processed = 0

  await mapWithConcurrency(names, SCAN_CONCURRENCY, async (name) => {
    processed++
    if (onProgress && processed % 200 === 0) {
      onProgress()
    }

    const packument = await fetchAbbreviatedPackument(name)
    if (isFetchError(packument)) return

    const latest = packument['dist-tags']?.latest
    const versionData = latest ? packument.versions?.[latest] : undefined
    if (!versionData) return

    for (const depKind of DEP_KINDS) {
      const deps = versionData[depKind] ?? {}
      const hasTarget = Object.entries(deps).some(([depName, depSpec]) =>
        targets.has(resolvedDependencyTarget(depName, depSpec).name),
      )
      if (hasTarget) {
        hits.push(name)
        return
      }
    }
  })

  return hits
}

export async function scanDependents(input: {
  vulnerablePackage: string
  relatedAffectedPackages: string[]
  vulnerableVersions: string[]
  topN: number
  scanLimit?: number
  onProgress?: () => void
}): Promise<ScanDependentsResult> {
  const {
    vulnerablePackage,
    relatedAffectedPackages,
    vulnerableVersions,
    topN,
    scanLimit,
    onProgress,
  } = input

  // Fetch high-impact names
  const names = await highImpactNames()
  const targets = new Set([vulnerablePackage, ...relatedAffectedPackages])
  const toScan = (scanLimit && scanLimit > 0 ? names.slice(0, scanLimit) : names).filter(
    (n) => !targets.has(n),
  )

  // Phase 1: concurrent, lightweight pre-scan — filters the (potentially ~17k) high-impact
  // list down to the names that actually declare a dependency on the target, before doing
  // any of the more expensive per-candidate work below.
  const candidateNames = await candidateNamesFromScan(toScan, targets, onProgress)

  // Fetch download counts (bulk, max 128 per request) for the last 30 days —
  // only for the filtered candidates, not the full high-impact list.
  const rangeEnd = new Date()
  const rangeStart = new Date(rangeEnd)
  rangeStart.setUTCDate(rangeStart.getUTCDate() - 30)
  const isoDate = (d: Date) => d.toISOString().slice(0, 10)

  const downloads = new Map<string, number>()
  const unscopedNames = candidateNames.filter((n) => !n.startsWith('@'))
  const scopedNames = candidateNames.filter((n) => n.startsWith('@'))

  // Neither loop below heartbeated before — for a large candidate pool (many
  // unscoped batches, or many scoped names run one HTTP call at a time through
  // mapWithConcurrency) this whole download-count phase could run past the
  // dependents stage's heartbeatTimeout with nothing heartbeating in between.
  for (let i = 0; i < unscopedNames.length; i += 128) {
    const batch = unscopedNames.slice(i, i + 128)
    const result = await fetchBulkPointRange(batch, isoDate(rangeStart), isoDate(rangeEnd))
    if (!isFetchError(result)) {
      result.counts.forEach((count: number, name: string) => {
        downloads.set(name, count)
      })
    }
    onProgress?.()
  }

  // fetchBulkPointRange doesn't support scoped package names; fetch those individually
  // so scoped candidates aren't silently ranked at 0 downloads and excluded from topN.
  let scopedProcessed = 0
  await mapWithConcurrency(scopedNames, SCAN_CONCURRENCY, async (name) => {
    const result = await fetchPointRange(name, isoDate(rangeStart), isoDate(rangeEnd))
    if (!isFetchError(result)) {
      downloads.set(name, result.count)
    }
    scopedProcessed++
    if (onProgress && scopedProcessed % 200 === 0) {
      onProgress()
    }
  })

  // Phase 2: walk candidates ranked by downloads (descending), fetching the full packument
  // only for names that made it through the phase-1 filter, until top-N pass the range check.
  const ranked = [...candidateNames].sort(
    (a, b) => (downloads.get(b) ?? 0) - (downloads.get(a) ?? 0),
  )

  const analyzed: DependentCandidate[] = []
  const excludedByRange: DependentCandidate[] = []

  for (const name of ranked) {
    if (analyzed.length >= topN) break

    // Phase 1 heartbeats every 200 candidates via mapWithConcurrency; this walk is
    // sequential (one packument fetch at a time), so it needs its own heartbeat too.
    onProgress?.()

    const packument = await fetchPackument(name)
    if (isFetchError(packument)) continue

    const latest = packument['dist-tags']?.latest
    if (!latest) continue

    const versionData = packument.versions?.[latest]
    if (!versionData) continue

    // Check for declared dependency
    const manifest = asNpmVersionManifest(versionData)
    const depInfo = declaredDependency(manifest, vulnerablePackage, relatedAffectedPackages)
    if (!depInfo) continue

    const { check, includes } = rangeIncludesAny(depInfo.range, vulnerableVersions)

    const candidate: DependentCandidate = {
      name,
      version: latest,
      downloads: downloads.get(name) ?? null,
      declaredRange: depInfo.range,
      dependencyKind: depInfo.kind,
      rangeIncludesVuln: includes,
      rangeCheck: check,
      tarballUrl: manifest.dist?.tarball ?? null,
    }

    if (includes) {
      analyzed.push(candidate)
      if (analyzed.length >= topN) break
    } else {
      excludedByRange.push(candidate)
    }
  }

  return {
    source: 'npm-high-impact',
    candidatesConsidered: candidateNames.length,
    analyzed,
    excludedByRange: excludedByRange.slice(0, 200),
    excludedByRangeCount: excludedByRange.length,
  }
}
