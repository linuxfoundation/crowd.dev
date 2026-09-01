export type PackageRepoSource = 'declared' | 'deps_dev' | 'heuristic' | 'manual'
export type PackageRepoSignal = 'primary' | 'secondary'
export type PackageRepoOwnershipMatch = 'matched' | 'unmatched' | 'no_evidence'
export type PackageRepoConfidenceLabel = 'high' | 'medium' | 'low'

export const CONFIDENCE_HIGH_THRESHOLD = 0.8
export const CONFIDENCE_MEDIUM_THRESHOLD = 0.5

export function packageRepoConfidenceLabel(confidence: number): PackageRepoConfidenceLabel {
  if (confidence >= CONFIDENCE_HIGH_THRESHOLD) return 'high'
  if (confidence >= CONFIDENCE_MEDIUM_THRESHOLD) return 'medium'
  return 'low'
}

export type PackageRepoLinkClaim = {
  source: PackageRepoSource
  signal?: PackageRepoSignal
  ownershipMatch?: PackageRepoOwnershipMatch
  provenance?: string | null
}

export function packageRepoLinkClaimParams(claim: PackageRepoLinkClaim): {
  source: PackageRepoSource
  signal: PackageRepoSignal
  ownershipMatch: PackageRepoOwnershipMatch
  provenance: string | null
} {
  return {
    source: claim.source,
    signal: claim.signal ?? 'primary',
    ownershipMatch: claim.ownershipMatch ?? 'no_evidence',
    provenance: claim.provenance ?? null,
  }
}

// A competing GitHub repo demotes a non-GitHub link (see package_repo_confidence).
// Correlated on the package and repo aliases the caller has in scope.
export function competingGithubRepoExpr(packageIdExpr: string, repoIdExpr: string): string {
  return `EXISTS (
        SELECT 1
          FROM package_repos competing
          JOIN repos competing_repo ON competing_repo.id = competing.repo_id
         WHERE competing.package_id = ${packageIdExpr}
           AND competing.repo_id <> ${repoIdExpr}
           AND competing_repo.host = 'github'
      )`
}

export type PackageRepoClaimExprs = {
  source: string
  signal: string
  ownershipMatch: string
  provenance: string
}

// A new claim's fields arrive as bound parameters; a rescore of existing rows reads them
// back off the stored row instead.
export const CLAIM_FROM_PARAMS: PackageRepoClaimExprs = {
  source: '$(source)',
  signal: '$(signal)',
  ownershipMatch: '$(ownershipMatch)',
  provenance: '$(provenance)',
}

export function claimFromRow(alias: string): PackageRepoClaimExprs {
  return {
    source: `${alias}.source`,
    signal: `${alias}.signal`,
    ownershipMatch: `${alias}.ownership_match`,
    provenance: `${alias}.provenance`,
  }
}

// Call into the scoring function defined in V1788307200 — the only path that may produce
// a package_repos confidence value. Ecosystem and repo state are read from the package
// and repo rows the caller has joined in.
export function packageRepoConfidenceCall(
  packageAlias: string,
  repoAlias: string,
  claim: PackageRepoClaimExprs = CLAIM_FROM_PARAMS,
): string {
  return `package_repo_confidence(
        ${claim.source}, ${packageAlias}.ecosystem, ${claim.signal}, ${claim.ownershipMatch}, ${claim.provenance},
        ${repoAlias}.archived, ${repoAlias}.is_fork, ${repoAlias}.disabled, ${repoAlias}.host,
        ${competingGithubRepoExpr(`${packageAlias}.id`, `${repoAlias}.id`)},
        ${repoAlias}.id
      )`
}
