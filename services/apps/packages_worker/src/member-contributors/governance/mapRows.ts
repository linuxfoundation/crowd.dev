import { canonicalRepoUrl } from '../../deps-dev/canonicalRepoUrl'
import { ContributorIdentityType } from '../types'

export const GOVERNANCE_FILE_SOURCE = 'governance_file'

export type RepoContributorIdentityType = ContributorIdentityType | 'git-author-name'

export interface CdpGovernanceRoleRow {
  id: string
  repoUrl: string
  role: string
  originalRole: string | null
  startDate: Date | null
  endDate: Date | null
  createdAt: Date
  identityPlatform: string
  identityType: string
  identityValue: string
}

export interface RepoContributorRow {
  repoId: string
  source: string
  role: string
  roleKind: string
  identityType: RepoContributorIdentityType
  identityValue: string
  firstSeenAt: Date
  lastSeenAt: Date
  endedAt: Date | null
}

const REPO_TYPE_BY_HOSTNAME: Record<string, string> = {
  'github.com': 'GITHUB',
  'gitlab.com': 'GITLAB',
  'bitbucket.org': 'BITBUCKET',
}

export function canonicalGovernanceRepoUrl(repoUrl: string): string | null {
  let parsed: URL
  try {
    parsed = new URL(repoUrl)
  } catch {
    return null
  }
  const type = REPO_TYPE_BY_HOSTNAME[parsed.hostname]
  if (!type) return null
  return canonicalRepoUrl(type, repoUrl)
}

export function classifyIdentity(
  platform: string,
  type: string,
  value: string,
): { identityType: RepoContributorIdentityType; identityValue: string } {
  const trimmed = value.trim()
  if (type === 'email' || trimmed.includes('@')) {
    return { identityType: 'email', identityValue: trimmed.toLowerCase() }
  }
  if (platform === 'github') {
    return { identityType: 'github-login', identityValue: trimmed.toLowerCase() }
  }
  return { identityType: 'git-author-name', identityValue: trimmed }
}

export function toRepoContributorRow(
  row: CdpGovernanceRoleRow,
  repoId: string,
  syncedAt: Date,
): RepoContributorRow {
  return {
    repoId,
    source: GOVERNANCE_FILE_SOURCE,
    role: row.originalRole ?? row.role,
    roleKind: row.role,
    ...classifyIdentity(row.identityPlatform, row.identityType, row.identityValue),
    firstSeenAt: row.startDate ?? row.createdAt,
    lastSeenAt: row.endDate ?? syncedAt,
    endedAt: row.endDate,
  }
}
