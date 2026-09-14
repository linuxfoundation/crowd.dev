import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { PrCommitNode } from '../graphql/pullRequestChildren'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

const DEFAULT_TIMESTAMP = '1970-01-01T00:00:00Z'

export function toCommit(commit: PrCommitNode['commit'], prId: string): GithubActivity {
  return {
    type: GithubActivityType.AUTHORED_COMMIT,
    timestamp: commit.authoredDate ?? DEFAULT_TIMESTAMP,
    sourceId: commit.oid,
    sourceParentId: prId,
    score: GITHUB_GRID[GithubActivityType.AUTHORED_COMMIT].score,
    body: commit.message,
    url: commit.url ?? '',
    attributes: {
      insertions: commit.additions,
      deletions: commit.deletions,
      // nango quirk kept for exact-match: lines = additions - deletions, not the sum
      lines: commit.additions - commit.deletions,
      isMerge: commit.parents.totalCount > 1,
    },
    member: toMember(commit.author?.user),
  }
}
