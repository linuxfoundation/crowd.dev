import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { DiscussionNode, DiscussionReplyNode } from '../graphql/discussions'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

const DEFAULT_TIMESTAMP = '1970-01-01T00:00:00Z'

export function toDiscussionStartedActivity(discussion: DiscussionNode): GithubActivity {
  return {
    type: GithubActivityType.DISCUSSION_STARTED,
    timestamp: discussion.createdAt ?? DEFAULT_TIMESTAMP,
    sourceId: discussion.id,
    score: GITHUB_GRID[GithubActivityType.DISCUSSION_STARTED].score,
    title: discussion.title || '',
    body: discussion.bodyText || '',
    url: discussion.url,
    attributes: {
      category: {
        id: String(discussion.category.id),
        isAnswerable: String(discussion.category.isAnswerable),
        name: discussion.category.name,
        slug: discussion.category.slug,
        emoji: discussion.category.emoji,
        description: discussion.category.description,
      },
    },
    member: toMember(discussion.author),
  }
}

export function toDiscussionCommentActivity(
  discussion: DiscussionNode,
  comment: DiscussionReplyNode,
  isReply: boolean,
): GithubActivity {
  return {
    type: GithubActivityType.DISCUSSION_COMMENT,
    timestamp: comment.createdAt ?? DEFAULT_TIMESTAMP,
    sourceId: comment.id,
    sourceParentId: discussion.id,
    score:
      discussion.isAnswered && !isReply
        ? GITHUB_GRID[GithubActivityType.DISCUSSION_COMMENT].score + 2
        : GITHUB_GRID[GithubActivityType.DISCUSSION_COMMENT].score,
    body: comment.bodyText || '',
    url: comment.url,
    attributes: { isAnswer: discussion.isAnswered ?? false },
    member: toMember(discussion.author),
  }
}
