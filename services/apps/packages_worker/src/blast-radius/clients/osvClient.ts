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
  versions?: string[]
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

// Flatten SEMVER-type ranges in an affected package into {introduced, fixed, lastAffected}
// triples. last_affected is OSV's inclusive upper bound (distinct from fixed, which is
// exclusive) — it closes the range at that version rather than leaving it open-ended.
// If OSV provides only a versions[] list (no ranges[]), convert each exact version into
// a degenerate range (introduced=v, last_affected=v) to match isInRange semantics.
export function semverRangeEvents(
  entry: OsvAffectedPackage,
): Array<{ introduced: string | null; fixed: string | null; lastAffected: string | null }> {
  const events: Array<{
    introduced: string | null
    fixed: string | null
    lastAffected: string | null
  }> = []

  if (entry.ranges) {
    for (const range of entry.ranges) {
      if (range.type !== 'SEMVER' || !range.events) continue

      let introduced: string | null = null

      for (const event of range.events) {
        if (event.introduced) introduced = event.introduced

        if (event.fixed) {
          events.push({ introduced, fixed: event.fixed, lastAffected: null })
          introduced = null
        } else if (event.last_affected) {
          events.push({ introduced, fixed: null, lastAffected: event.last_affected })
          introduced = null
        }
      }

      // Trailing open range: introduced but never closed by fixed/last_affected.
      if (introduced !== null) {
        events.push({ introduced, fixed: null, lastAffected: null })
      }
    }
  }

  // Fallback: if no SEMVER ranges, use explicit version list as exact vulnerable versions.
  if (events.length === 0 && (entry.versions ?? []).length > 0) {
    for (const v of entry.versions ?? []) {
      events.push({ introduced: v, fixed: null, lastAffected: v })
    }
  }

  return events
}

// True only when the URL's host is github.com or a github.com subdomain — a substring
// check like `url.includes('github.com')` would also match hostiles such as
// `evil.com/?x=github.com` or `github.com.evil.com`.
function isGithubUrl(url: string): boolean {
  try {
    const { hostname } = new URL(url)
    return hostname === 'github.com' || hostname.endsWith('.github.com')
  } catch {
    return false
  }
}

// Extract the first FIX-type GitHub reference (commit or PR), then other web refs.
export function fixReferenceUrls(vuln: OsvVuln): string[] {
  const results: string[] = []

  // FIX-type refs first
  if (vuln.references) {
    for (const ref of vuln.references) {
      if (ref.type === 'FIX' && isGithubUrl(ref.url)) {
        results.push(ref.url)
      }
    }
  }

  // Then other GitHub refs. Restricted to github.com like the FIX-type pass above —
  // references are attacker-influenced OSV advisory data and feed into fetchPatch,
  // which fetches whatever URL it's given.
  if (vuln.references) {
    for (const ref of vuln.references) {
      if (ref.type !== 'FIX' && isGithubUrl(ref.url)) {
        results.push(ref.url)
      }
    }
  }

  return results
}
