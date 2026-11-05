import { isBotAccount } from '../../utils/githubBots'
import { GIT_ACTIVITY_ROLE, GIT_ACTIVITY_ROLE_KIND } from '../git-activity/mapRows'
import { RepoContributorRow } from '../governance/mapRows'

export const GITHUB_SOURCE = 'github'

export type GithubContributorsStatus = 'fetched' | 'missing' | 'empty' | 'error'

export interface GithubContributor {
  login?: unknown
  type?: unknown
  contributions?: unknown
}

function contributorLogin(contributor: GithubContributor): string | null {
  if (typeof contributor.login !== 'string') return null
  const login = contributor.login.trim()
  if (login.length === 0 || isBotAccount(login, contributor.type)) return null
  return login.toLowerCase()
}

export function toGithubContributorLogins(contributors: GithubContributor[]): string[] {
  const logins = new Set<string>()
  for (const contributor of contributors) {
    const login = contributorLogin(contributor)
    if (login !== null) logins.add(login)
  }
  return [...logins]
}

export function toGithubContributorRows(
  contributors: GithubContributor[],
  repoId: string,
  observedAt: Date,
): RepoContributorRow[] {
  return toGithubContributorLogins(contributors).map((login) => ({
    repoId,
    source: GITHUB_SOURCE,
    role: GIT_ACTIVITY_ROLE,
    roleKind: GIT_ACTIVITY_ROLE_KIND,
    identityType: 'github-login',
    identityValue: login,
    firstSeenAt: observedAt,
    lastSeenAt: observedAt,
    endedAt: null,
  }))
}
