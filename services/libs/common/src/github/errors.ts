// Thrown by GitHub REST response classification for installation-token-authenticated calls
// (star_snapshot_worker's backfill client). err.name is stored verbatim as lastErrorClass on
// repositoryStarBackfillStatus, so findDeadLetteredStarBackfillFailures can exclude the
// non-actionable ones by name - kept in this one file so the two stay in sync automatically.
export class GithubRepoNotFoundError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'GithubRepoNotFoundError'
  }
}

export class GithubIpAllowlistError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'GithubIpAllowlistError'
  }
}

export class GithubForbiddenError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'GithubForbiddenError'
  }
}

export class GithubAuthError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'GithubAuthError'
  }
}

// Permanent external conditions (repo gone, org IP allow list policy), not our bugs - excluded
// from the star snapshot dead-letter Slack report.
export const NON_ACTIONABLE_GITHUB_ERROR_CLASSES: string[] = [
  GithubRepoNotFoundError.name,
  GithubIpAllowlistError.name,
]
