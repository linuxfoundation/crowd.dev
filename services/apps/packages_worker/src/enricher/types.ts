export interface LightRepoResult {
  url: string
  host: 'github'
  owner: string
  name: string
  description: string | null
  primaryLanguage: string | null
  topics: string[]
  stars: number
  forks: number
  watchers: number
  openIssues: number
  lastCommitAt: string | null
  archived: boolean
  disabled: boolean
  isFork: boolean
  createdAt: string | null
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
