import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { PrCommentNode } from '../graphql/pullRequestChildren'
import type { PullRequestNode } from '../graphql/pullRequests'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

export function toPrComment(comment: PrCommentNode, pullRequest: PullRequestNode): GithubActivity {
  return {
    type: GithubActivityType.PULL_REQUEST_COMMENT,
    timestamp: comment.createdAt,
    sourceId: comment.id,
    sourceParentId: pullRequest.id,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_COMMENT].score,
    title: pullRequest.title || '',
    body: comment.body || '',
    url: pullRequest.url,
    attributes: {
      state: pullRequest.state,
      additions: pullRequest.additions,
      deletions: pullRequest.deletions,
      changedFiles: pullRequest.changedFiles,
      authorAssociation: pullRequest.authorAssociation,
      labels: pullRequest.labels?.nodes?.map((label) => label.name) || [],
    },
    member: toMember(comment.author),
  }
}
