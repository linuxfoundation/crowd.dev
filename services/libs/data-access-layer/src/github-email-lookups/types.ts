export type GithubEmailLookupResult =
  | 'merged'
  | 'identity-added'
  | 'no-merge'
  | 'github-id-mismatch'
  | 'no-github-account'
  | 'commit-not-found'
  | 'bot-account'
  | 'repo-inaccessible'

export interface IGithubEmailLookup {
  email: string
  result: GithubEmailLookupResult
  githubId: string | null
  githubLogin: string | null
  commitSha: string
}

export interface IGitAuthorCommit {
  repoUrl: string
  commitSha: string
}

export interface IGitAuthor extends IGitAuthorCommit {
  memberId: string
  email: string
  integrationId: string | null
}

export interface IGitAuthorsPage {
  authors: IGitAuthor[]
  nextEmail: string | null
}

export interface IGithubAccountOwner {
  memberId: string
  githubId: string | null
  isBot: boolean
}
