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
  /** Username an A3 email was resolved from — used only to identity-link a bare github-handle. */
  handle?: string
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
  /** From the enricher; null = not yet enriched. Archived repos can't be queried for PVR. */
  archived: boolean | null
  packages: RepoPackage[]
}

/** Result of a GitHub API GET routed through the rate-limit-aware pool. */
export interface GithubGetResult {
  status: number
  /** Response body (raw file text or JSON string); null for absent resources (404/410/422). */
  text: string | null
}

export interface ExtractorDeps {
  fetchTimeoutMs: number
  /** Sent on registry calls; required (crates.io rejects requests without an identifying UA). */
  userAgent: string
  /**
   * Pool-aware, rate-limit-safe GitHub API GET. Handles installation selection, budget parking,
   * and 429/secondary-limit backoff internally. `raw` selects the raw media type (file contents).
   */
  githubGet: (path: string, opts?: { raw?: boolean }) => Promise<GithubGetResult>
}

export type Extractor = (target: RepoTarget, deps: ExtractorDeps) => Promise<ExtractorResult>
