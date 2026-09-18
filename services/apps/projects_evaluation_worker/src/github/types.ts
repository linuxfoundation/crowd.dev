export type GithubPublicClientErrorKind =
  | 'CONFIG'
  | 'AUTH'
  | 'RATE_LIMIT'
  | 'NOT_FOUND'
  | 'TRANSIENT'

export class GithubPublicClientError extends Error {
  constructor(
    public readonly kind: GithubPublicClientErrorKind,
    message: string,
    public readonly resetAtMs?: number,
  ) {
    super(message)
    this.name = 'GithubPublicClientError'
  }
}

export interface IPublicRepoReadme {
  content: string
  truncated: boolean
}

export interface IPublicRepoMetrics {
  description: string | null
  primaryLanguage: string | null
  stars: number
  forks: number
  openIssues: number
  closedIssues: number
  openPullRequests: number
  closedPullRequests: number
  pushedAt: string | null
  createdAt: string
  isArchived: boolean
  isFork: boolean
}
