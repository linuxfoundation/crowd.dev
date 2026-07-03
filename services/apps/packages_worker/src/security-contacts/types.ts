import type { SecurityContactConfidence } from '@crowd/data-access-layer/src/osspckgs/api'

export type ContactChannel = 'email' | 'github-pvr' | 'url' | 'github-handle' | 'web-form'

export type ContactRole = 'security-team' | 'maintainer' | 'admin' | 'committer' | 'org-owner'

export type SourceTier = 'A' | 'B' | 'C' | 'D'

/** Where a single contact value was observed, for auditability and corroboration scoring. */
export interface ProvenanceEntry {
  source: string
  sourceTier: SourceTier
  path?: string
  fetchedAt: string
  /** When the source itself declared the value, if different from fetchedAt. ISO-8601. */
  declaredAt?: string
}

export interface RawContact {
  channel: ContactChannel
  value: string
  role: ContactRole
  name?: string
  /** Username an email was resolved from — used to identity-link a bare github-handle. */
  handle?: string
  tier: SourceTier
  provenance: ProvenanceEntry[]
}

export interface ScoredContact extends RawContact {
  score: number
  confidence: SecurityContactConfidence
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

export interface GithubGetResult {
  status: number
  /** Null for absent resources (404/410/422/451). */
  text: string | null
}

export interface ExtractorDeps {
  fetchTimeoutMs: number
  /** Required — crates.io rejects requests without an identifying UA. */
  userAgent: string
  githubGet: (path: string, opts?: { raw?: boolean }) => Promise<GithubGetResult>
  /** Default-branch file paths from one git-tree call; null means unresolved — probe as before. */
  repoTree: { paths: Set<string> | null }
}

export type Extractor = (target: RepoTarget, deps: ExtractorDeps) => Promise<ExtractorResult>
