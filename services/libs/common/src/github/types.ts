export type GithubPublicClientErrorKind =
  | 'CONFIG'
  | 'AUTH'
  | 'ACCESS_DENIED'
  | 'RATE_LIMIT'
  | 'NOT_FOUND'
  | 'TRANSIENT'

export interface IPublicOrgRepos {
  repoUrls: string[]
  exceededLimit: boolean
}

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

export interface IGithubAccount {
  id: string
  nodeId: string
  login: string
}

export type GithubCommitAuthor =
  | { status: 'found'; account: IGithubAccount }
  | { status: 'bot-account'; account: IGithubAccount }
  | { status: 'no-github-account' }
  | { status: 'commit-not-found' }
  | { status: 'repo-inaccessible' }

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
  hasIssuesEnabled: boolean
  openPullRequests: number
  closedPullRequests: number
  pushedAt: string | null
  createdAt: string
  isArchived: boolean
  isFork: boolean
  parentFullName: string | null
}
