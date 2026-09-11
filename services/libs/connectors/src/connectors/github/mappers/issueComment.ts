import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { IssueCommentNode, IssueNode } from '../graphql/issues'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

export function toIssueComment(comment: IssueCommentNode, issue: IssueNode): GithubActivity {
  return {
    type: GithubActivityType.ISSUE_COMMENT,
    timestamp: comment.createdAt,
    sourceId: comment.id,
    sourceParentId: issue.id,
    score: GITHUB_GRID[GithubActivityType.ISSUE_COMMENT].score,
    title: issue.title || '',
    body: comment.bodyText || '',
    url: comment.url || issue.url,
    attributes: { state: issue.state.toLowerCase() },
    member: toMember(comment.author),
  }
}
