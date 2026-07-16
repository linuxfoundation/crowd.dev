// repos: one row per resolved code repository. Base shape from
// backend/src/osspckgs/migrations/V1779710880__initial_schema.sql; remaining fields
// added by later ALTER TABLE migrations under backend/src/osspckgs/migrations/
// (scorecard, branch-protection, security/contacts columns).
export interface RepoDbRow {
  id: string
  url: string
  host: string | null
  owner: string | null
  name: string | null
  description: string | null
  primaryLanguage: string | null
  topics: string[] | null
  stars: number | null
  forks: number | null
  watchers: number | null
  openIssues: number | null
  lastCommitAt: string | null
  archived: boolean | null
  disabled: boolean | null
  isFork: boolean | null
  createdAt: string | null
  homepage: string | null
  rawProjectType: string | null
  rawProjectName: string | null
  // NUMERIC(3,1) — parsed to a number by @crowd/database's OID 1700 type-parser override,
  // same convention as SecurityContactDbRow.score.
  scorecardScore: number | null
  scorecardLastRunAt: string | null
  lastSyncedAt: string | null
  // -- added by later migrations --
  branchProtectionAllowsForcePush: boolean | null
  branchProtectionEnabled: boolean | null
  branchProtectionRequiredReviews: number | null
  branchProtectionRequiresStatusChecks: boolean | null
  securityFileEnabled: boolean | null
  securityPolicyEnabled: boolean | null
  skipEnrichment: boolean
  snapshotAt: string | null
  // -- added by V1782950400__security_contacts.sql --
  pvrEnabled: boolean | null
  securityPolicyUrl: string | null
  vulnerabilityReportingUrl: string | null
  bugBountyUrl: string | null
  securityTxtUrl: string | null
  contactsLastRefreshed: string | null
}
