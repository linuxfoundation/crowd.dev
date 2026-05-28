// ---- Raw OSV JSON (only the fields we read) ----
// Reference: https://ossf.github.io/osv-schema/

export interface OsvSeverity {
  // 'CVSS_V2' | 'CVSS_V3' | 'CVSS_V4' | other future types
  type: string
  // The CVSS vector string, not a numeric score.
  score: string
}

export interface OsvRangeEvent {
  introduced?: string
  fixed?: string
  last_affected?: string
  limit?: string
}

export interface OsvRange {
  // 'SEMVER' | 'ECOSYSTEM' | 'GIT'
  type: string
  events: OsvRangeEvent[]
}

export interface OsvAffected {
  package?: {
    ecosystem: string
    name: string
    purl?: string
  }
  ranges?: OsvRange[]
  versions?: string[]
  database_specific?: Record<string, unknown>
}

export interface OsvRecord {
  id: string
  aliases?: string[]
  summary?: string
  details?: string
  published?: string
  modified?: string
  affected?: OsvAffected[]
  severity?: OsvSeverity[]
  database_specific?: {
    severity?: string
    [k: string]: unknown
  }
  references?: Array<{ type?: string; url?: string }>
  schema_version?: string
}

// ---- Normalized rows we write to packages-db ----

export type CvssSource =
  | 'osv_cvss_v3'
  | 'osv_cvss_v4'
  | 'osv_qualitative_fallback'
  | 'osv_malicious_package'

export interface NormalizedAdvisory {
  osvId: string
  // ADR-0001 §OSV / advisories.source — 'OSV' for everything ingested by this
  // worker. The granular 'GHSA' | 'NVD' | 'NSWG' attribution is the deps.dev
  // BQ ingestion worker's responsibility, not this one.
  source: string
  sourceUrl: string | null
  aliases: string[]
  severity: string | null
  cvss: number | null
  cvssSource: CvssSource | null
  summary: string | null
  details: string | null
  publishedAt: string | null
  modifiedAt: string | null
}

export interface NormalizedAdvisoryPackage {
  ecosystem: string
  packageName: string
  // namespace/name pair used to resolve packages.id; mirrors the COALESCE-aware
  // unique index on packages(ecosystem, COALESCE(namespace,''), name).
  namespace: string | null
  name: string
}

export interface NormalizedRange {
  introducedVersion: string | null
  fixedVersion: string | null
  lastAffected: string | null
}

export interface NormalizedPackageEntry {
  pkg: NormalizedAdvisoryPackage
  ranges: NormalizedRange[]
}

export interface NormalizedRecord {
  advisory: NormalizedAdvisory
  packages: NormalizedPackageEntry[]
}

// ---- Error type, mirrors enricher/types.ts ----

export type FetchErrorKind = 'NETWORK' | 'NOT_FOUND' | 'PARSE' | 'TRANSIENT'

export class FetchError extends Error {
  constructor(
    public readonly kind: FetchErrorKind,
    message: string,
    public readonly resetAt?: number,
  ) {
    super(message)
    this.name = 'FetchError'
  }
}
