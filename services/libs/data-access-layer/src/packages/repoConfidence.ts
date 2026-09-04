export type PackageRepoSource = 'declared' | 'deps_dev' | 'heuristic' | 'manual'
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
  provenance?: string | null
}

export function packageRepoLinkClaimParams(claim: PackageRepoLinkClaim): {
  source: PackageRepoSource
  provenance: string | null
} {
  return {
    source: claim.source,
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
  provenance: string
}

// A new claim's fields arrive as bound parameters; a rescore of existing rows reads them
// back off the stored row instead.
export const CLAIM_FROM_PARAMS: PackageRepoClaimExprs = {
  source: '$(source)',
  provenance: '$(provenance)',
}

export function claimFromRow(alias: string): PackageRepoClaimExprs {
  return {
    source: `${alias}.source`,
    provenance: `${alias}.provenance`,
  }
}

// Keep-highest arbitrates between different sources only: a claim from another source
// replaces the stored one when it scores strictly higher, which makes the result
// independent of the order the writers run in. A same-source write is that source
// refreshing its own row, so it always replaces — otherwise evidence that got weaker
// (a deps.dev link that lost its attestation) could never be persisted, and a row
// written before `provenance` existed could never acquire one.
export const KEEP_HIGHEST_CONFLICT_UPDATE = `source           = CASE WHEN EXCLUDED.confidence > package_repos.confidence
                                 THEN EXCLUDED.source ELSE package_repos.source END,
         provenance       = CASE WHEN EXCLUDED.source = package_repos.source
                                   OR EXCLUDED.confidence > package_repos.confidence
                                 THEN EXCLUDED.provenance ELSE package_repos.provenance END,
         confidence       = CASE WHEN EXCLUDED.source = package_repos.source
                                 THEN EXCLUDED.confidence
                                 ELSE GREATEST(EXCLUDED.confidence, package_repos.confidence) END,
         verified_at      = NOW()`

// Call into the scoring function defined in V1788307200 — the only path that may produce
// a package_repos confidence value. Ecosystem and repo state are read from the package
// and repo rows the caller has joined in.
export function packageRepoConfidenceCall(
  packageAlias: string,
  repoAlias: string,
  claim: PackageRepoClaimExprs = CLAIM_FROM_PARAMS,
  competingGithubExpr?: string,
): string {
  const competing =
    competingGithubExpr ?? competingGithubRepoExpr(`${packageAlias}.id`, `${repoAlias}.id`)
  return `package_repo_confidence(
        ${claim.source}, ${packageAlias}.ecosystem, ${claim.provenance},
        ${repoAlias}.archived, ${repoAlias}.is_fork, ${repoAlias}.disabled, ${repoAlias}.host,
        ${competing},
        ${repoAlias}.id
      )`
}
