export interface LightRepoResult {
  url: string
  host: 'github'
  owner: string
  name: string
  description: string | null
  primaryLanguage: string | null
  topics: string[]
  stars: number | null
  forks: number | null
  watchers: number | null
  openIssues: number | null
  lastCommitAt: string | null
  archived: boolean | null
  disabled: boolean | null
  isFork: boolean | null
  createdAt: string | null
  securityPolicyEnabled: boolean | null
  securityFileEnabled: boolean | null
  rateLimit: {
    limit: number
    cost: number
    remaining: number
    resetAt: string
  } | null
}

export interface RepoActivitySnapshot {
  repoId: string
  snapshotAt: string
  windowMonths: number
  commitsLast12m: number | null
  commitsLast6m: number | null
  commitsPrior6m: number | null
  prsOpenedLast12m: number | null
  prsMergedLast12m: number | null
  prsClosedUnmerged12m: number | null
  prMedianTimeToMergeHours: number | null
  prMedianTimeToFirstResponseHours: number | null
  issuesOpenedLast12m: number | null
  issuesClosedLast12m: number | null
  issuesOpenedLast6m: number | null
  issuesOpenedPrior6m: number | null
  issuesOpenNow: number | null
  issueMedianTimeToCloseHours: number | null
  issueMedianTimeToFirstResponseHours: number | null
  rateLimitCost: number
}

export type FetchErrorKind = 'RATE_LIMIT' | 'TRANSIENT' | 'NOT_FOUND' | 'AUTH' | 'MALFORMED'

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
