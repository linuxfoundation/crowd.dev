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
  rateLimit: {
    limit: number
    cost: number
    remaining: number
    resetAt: string
  } | null
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
