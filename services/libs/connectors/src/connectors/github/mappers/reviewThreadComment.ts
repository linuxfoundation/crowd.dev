import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { ReviewThreadNode, ThreadCommentNode } from '../graphql/pullRequestChildren'
import type { PullRequestNode } from '../graphql/pullRequests'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

export function toReviewThreadComment(
  comment: ThreadCommentNode,
  thread: ReviewThreadNode,
  pullRequest: PullRequestNode,
): GithubActivity {
  return {
    type: GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT,
    timestamp: comment.createdAt,
    sourceId: comment.id,
    sourceParentId: pullRequest.id,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT].score,
    title: pullRequest.title || '',
    body: `[Thread ${thread.isResolved ? 'RESOLVED' : 'OPEN'}] ${comment.body || ''}`,
    url: comment.url || pullRequest.url,
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
