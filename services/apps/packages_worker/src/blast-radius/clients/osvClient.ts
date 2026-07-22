// OSV.dev API client for fetching vulnerability records by GHSA/CVE ID.

export interface OsvAffectedPackage {
  package: {
    ecosystem: string
    name: string
  }
  ranges?: Array<{
    type: string
    events?: Array<{
      introduced?: string
      fixed?: string
      last_affected?: string
    }>
  }>
}

export interface OsvVuln {
  id: string
  aliases: string[]
  summary?: string
  details?: string
  references: Array<{
    type?: string
    url: string
  }>
  affected?: OsvAffectedPackage[]
  modified?: string
  published?: string
}

const OSV_API_BASE = 'https://api.osv.dev/v1'

export async function fetchOsvVuln(vulnId: string): Promise<OsvVuln> {
  const url = `${OSV_API_BASE}/vulns/${encodeURIComponent(vulnId)}`
  const res = await fetch(url)

  if (!res.ok) {
    throw new Error(`OSV.dev fetch failed: ${res.status} ${res.statusText}`)
  }

  return (await res.json()) as OsvVuln
}

// Extract npm-specific affected packages from an OSV record.
export function affectedNpmEntries(vuln: OsvVuln): OsvAffectedPackage[] {
  if (!vuln.affected) return []
  return vuln.affected.filter((a) => a.package?.ecosystem === 'npm')
}

// Flatten SEMVER-type ranges in an affected package into {introduced, fixed} pairs.
// last_affected events clear the fixed version (making the range open-ended).
export function semverRangeEvents(
  entry: OsvAffectedPackage,
): Array<{ introduced: string | null; fixed: string | null }> {
  if (!entry.ranges) return []

  const events: Array<{ introduced: string | null; fixed: string | null }> = []
  for (const range of entry.ranges) {
    if (range.type !== 'SEMVER' || !range.events) continue

    let introduced: string | null = null

    for (const event of range.events) {
      if (event.introduced) introduced = event.introduced

      if (event.fixed) {
        events.push({ introduced, fixed: event.fixed })
        introduced = null
      } else if (event.last_affected) {
        events.push({ introduced, fixed: null })
        introduced = null
      }
    }

    // Trailing open range: introduced but never closed by fixed/last_affected.
    if (introduced !== null) {
      events.push({ introduced, fixed: null })
    }
  }

  return events
}

// Extract the first FIX-type GitHub reference (commit or PR), then other web refs.
export function fixReferenceUrls(vuln: OsvVuln): string[] {
  const results: string[] = []

  // FIX-type refs first
  if (vuln.references) {
    for (const ref of vuln.references) {
      if (ref.type === 'FIX' && ref.url.includes('github.com')) {
        results.push(ref.url)
      }
    }
  }

  // Then other web refs
  if (vuln.references) {
    for (const ref of vuln.references) {
      if (ref.type !== 'FIX' && ref.url.startsWith('http')) {
        results.push(ref.url)
      }
    }
  }

  return results
}
