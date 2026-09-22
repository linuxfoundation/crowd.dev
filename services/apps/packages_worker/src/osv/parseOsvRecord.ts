import { extractSeverity } from './extractSeverity'
import {
  NormalizedAdvisoryPackage,
  NormalizedPackageEntry,
  NormalizedRange,
  NormalizedRecord,
  OsvAffected,
  OsvRecord,
} from './types'

// Splits an OSV `package.name` into the (namespace, name) pair that matches
// the packages table's unique index on (ecosystem, COALESCE(namespace,''), name).
// - npm: '@scope/pkg' -> namespace='scope', name='pkg'; bare 'pkg' -> (null,'pkg')
// - maven: 'groupId:artifactId' -> namespace='groupId', name='artifactId'
// - Other ecosystems: (null, name); we never reach this in v1 due to the allowlist.
//
// Ecosystem strings here are already lowercased (see parseOsvRecord) per the
// ADR-0001 §OSV "Ecosystem normalization" rule.
function splitName(ecosystem: string, rawName: string): { namespace: string | null; name: string } {
  if (ecosystem === 'maven') {
    const colon = rawName.indexOf(':')
    if (colon > 0) {
      return { namespace: rawName.slice(0, colon), name: rawName.slice(colon + 1) }
    }
    return { namespace: null, name: rawName }
  }

  if (ecosystem === 'npm' && rawName.startsWith('@')) {
    const slash = rawName.indexOf('/')
    if (slash > 1) {
      return { namespace: rawName.slice(1, slash), name: rawName.slice(slash + 1) }
    }
  }

  return { namespace: null, name: rawName }
}

// Builds the canonical OSV advisory URL for the source_url column. Deterministic
// per ADR-0001 §OSV (`source` and `source_url` columns are part of the
// advisories contract); the granular GHSA/NVD/NSWG attribution that BQ
// ingestion will populate comes through a different worker.
function osvSourceUrl(id: string): string {
  return `https://osv.dev/vulnerability/${id}`
}

// Reduces an OSV `affected[i].ranges[]` block into the flat shape we store.
// Each OSV range is a sequence of events along a single line; we collapse
// each contiguous (introduced -> fixed | last_affected) span into one row.
// Events for unrelated ranges live in separate `ranges[]` entries, so we
// process them independently.
function flattenRanges(affected: OsvAffected): NormalizedRange[] {
  const out: NormalizedRange[] = []

  for (const range of affected.ranges ?? []) {
    // GIT ranges are commit-hash bounds and not useful for version-based
    // vulnerability matching; skip them.
    if (range.type === 'GIT') continue

    let introduced: string | null = null
    for (const event of range.events ?? []) {
      if (event.introduced !== undefined) {
        // Flush a prior open introduced= row that never saw a fixed/last_affected.
        if (introduced !== null) {
          out.push({ introducedVersion: introduced, fixedVersion: null, lastAffected: null })
        }
        // OSV uses '0' to mean "from the beginning"; preserve it as-is so the
        // comparator can treat it as "always vulnerable" (decision #6 / MAL- case).
        introduced = event.introduced
      } else if (event.fixed !== undefined) {
        out.push({
          introducedVersion: introduced,
          fixedVersion: event.fixed,
          lastAffected: null,
        })
        introduced = null
      } else if (event.last_affected !== undefined) {
        out.push({
          introducedVersion: introduced,
          fixedVersion: null,
          lastAffected: event.last_affected,
        })
        introduced = null
      }
      // `limit` events bound the range scan but do not themselves define a
      // vulnerable window; ignore.
    }
    if (introduced !== null) {
      out.push({ introducedVersion: introduced, fixedVersion: null, lastAffected: null })
    }
  }

  return out
}

// parseOsvRecord normalizes a single OSV JSON record into the shape we write.
// - applies the ecosystem allowlist (records whose only affected[] entries are
//   outside the allowlist return packages: []; upsertAdvisory skips writing them)
// - splits names per-ecosystem so package_id resolution can join via
//   (ecosystem, COALESCE(namespace,''), name)
// - preserves the advisory metadata even when no affected packages survive the
//   filter, so callers can decide to skip vs. log
export function parseOsvRecord(
  record: OsvRecord,
  allowedEcosystems: Set<string>,
): NormalizedRecord {
  const severity = extractSeverity(record)

  // Group affected[] entries by (ecosystem, package_name) — OSV may list the
  // same package multiple times when there are disjoint affected ranges.
  const seen = new Map<string, NormalizedPackageEntry>()
  for (const affected of record.affected ?? []) {
    const pkg = affected.package
    if (!pkg) continue
    const rawEcosystem = pkg.ecosystem.toLowerCase()
    const ecosystem = rawEcosystem === 'crates.io' ? 'cargo' : rawEcosystem
    if (!allowedEcosystems.has(ecosystem)) continue

    const ranges = flattenRanges(affected)
    // If OSV only provides an enumerated `versions[]` list (no `ranges[]`),
    // convert each exact version into a degenerate range (introduced=v,
    // last_affected=v) so isInRange matches when `latest_version === v`.
    // Without this we'd record the advisory_package but no range rows, and
    // deriveCriticalFlag would silently fail to flag the package.
    if (ranges.length === 0 && (affected.versions ?? []).length > 0) {
      for (const v of affected.versions ?? []) {
        ranges.push({ introducedVersion: v, fixedVersion: null, lastAffected: v })
      }
    }
    if (ranges.length === 0) {
      // No usable affected range at all; skip this package entry but keep going.
      continue
    }

    const key = `${ecosystem} ${pkg.name}`
    const split = splitName(ecosystem, pkg.name)
    const pkgRow: NormalizedAdvisoryPackage = {
      ecosystem,
      packageName: pkg.name,
      namespace: split.namespace,
      name: split.name,
    }

    const existing = seen.get(key)
    if (existing) {
      existing.ranges.push(...ranges)
    } else {
      seen.set(key, { pkg: pkgRow, ranges })
    }
  }

  return {
    advisory: {
      osvId: record.id,
      source: 'OSV',
      sourceUrl: osvSourceUrl(record.id),
      aliases: record.aliases ?? [],
      severity: severity.severity,
      cvss: severity.cvss,
      cvssSource: severity.cvssSource,
      summary: record.summary ?? null,
      details: record.details ?? null,
      publishedAt: record.published ?? null,
      modifiedAt: record.modified ?? null,
    },
    packages: [...seen.values()],
  }
}
