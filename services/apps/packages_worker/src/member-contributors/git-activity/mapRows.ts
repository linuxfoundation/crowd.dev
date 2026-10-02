import { RepoContributorRow, classifyIdentity } from '../governance/mapRows'

export const GIT_ACTIVITY_SOURCE = 'git_activity'
export const GIT_ACTIVITY_ROLE = 'commit-author'
export const GIT_ACTIVITY_ROLE_KIND = 'contributor'

export interface TinybirdCommitContributorRow {
  channel: string
  memberId: string
  platform: string
  username: string
  firstCommitAt: string
  lastCommitAt: string
  lastUpdatedAt: string
}

export interface GitActivityContributorRow extends RepoContributorRow {
  cdpMemberId: string
}

export function parseTinybirdDateTime(value: string): Date {
  const parsed = new Date(`${value.replace(' ', 'T')}Z`)
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Unparseable Tinybird datetime: ${value}`)
  }
  return parsed
}

export function toGitActivityContributorRow(
  row: TinybirdCommitContributorRow,
  repoId: string,
): GitActivityContributorRow {
  return {
    repoId,
    source: GIT_ACTIVITY_SOURCE,
    role: GIT_ACTIVITY_ROLE,
    roleKind: GIT_ACTIVITY_ROLE_KIND,
    ...classifyIdentity(row.platform, 'username', row.username),
    firstSeenAt: parseTinybirdDateTime(row.firstCommitAt),
    lastSeenAt: parseTinybirdDateTime(row.lastCommitAt),
    endedAt: null,
    cdpMemberId: row.memberId,
  }
}
