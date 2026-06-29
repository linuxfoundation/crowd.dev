export type ContactChannel = 'email' | 'github-pvr' | 'url' | 'github-handle' | 'web-form'

export type ContactRole = 'security-team' | 'maintainer' | 'admin' | 'committer' | 'org-owner'

export type ConfidenceBand = 'PRIMARY' | 'SECONDARY' | 'FALLBACK' | 'NONE'

export type SourceTier = 'A' | 'B' | 'C' | 'D'

/** Where a single contact value was observed, for auditability and corroboration scoring. */
export interface ProvenanceEntry {
  /** Human-readable source identifier, e.g. 'security-insights', 'pvr', 'security.md'. */
  source: string
  sourceTier: SourceTier
  /** Path or URL the value was read from, when applicable. */
  path?: string
  /** When this worker fetched the source. ISO-8601. */
  fetchedAt: string
  /** When the source itself declared the value (e.g. SECURITY-INSIGHTS last-updated). ISO-8601. */
  declaredAt?: string
}

/** Extractor output, before reconciliation and scoring. */
export interface RawContact {
  channel: ContactChannel
  value: string
  role: ContactRole
  name?: string
  tier: SourceTier
  provenance: ProvenanceEntry[]
}

export interface ScoredContact extends RawContact {
  score: number
  confidence: ConfidenceBand
}

export interface RepoPolicies {
  securityPolicyUrl?: string
  vulnerabilityReportingUrl?: string
  bugBountyUrl?: string
  securityTxtUrl?: string
  pvrEnabled?: boolean
}

export interface ExtractorResult {
  contacts: RawContact[]
  policies: Partial<RepoPolicies>
}

export interface RepoPackage {
  purl: string
  ecosystem: string
}

export interface RepoTarget {
  repoId: string
  url: string
  homepage: string | null
  packages: RepoPackage[]
}

export interface ExtractorDeps {
  fetchTimeoutMs: number
  /** Mints a GitHub installation token; only the authed extractors (A2/A3) use it. */
  getToken?: () => Promise<string>
}

export type Extractor = (target: RepoTarget, deps: ExtractorDeps) => Promise<ExtractorResult>
