import { mapWithConcurrency } from '../../../concurrency'
import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type {
  CommentRepliesPage,
  DiscussionCommentNode,
  DiscussionCommentsPage,
  DiscussionNode,
  DiscussionReplyNode,
  DiscussionsPage,
} from '../graphql/discussions'
import {
  COMMENT_REPLIES_QUERY,
  DISCUSSIONS_QUERY,
  DISCUSSION_COMMENTS_QUERY,
} from '../graphql/discussions'
import { toDiscussionCommentActivity, toDiscussionStartedActivity } from '../mappers/discussion'
import { ITEM_FETCH_CONCURRENCY, PAGE_SIZE, parseRepoChannel, readWatermark } from '../paging'
import type { GithubActivity } from '../schemas'
import { githubActivitySchema } from '../schemas'

const COMMENTS_PAGE_SIZE = 50
const REPLIES_PAGE_SIZE = 100

async function runDiscussionsSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  const sinceDate = watermark.since ? new Date(watermark.since) : null
  let newestUpdatedAt = watermark.since
  let cursor: string | null = null

  const collectReplies = async (
    discussion: DiscussionNode,
    comment: DiscussionCommentNode,
  ): Promise<GithubActivity[]> => {
    const activities = comment.replies.nodes
      .filter((node): node is DiscussionReplyNode => node !== null)
      .map((reply) => toDiscussionCommentActivity(discussion, reply, true))

    let repliesCursor = comment.replies.pageInfo.hasNextPage
      ? comment.replies.pageInfo.endCursor
      : null
    while (repliesCursor) {
      const data = await githubGraphql<CommentRepliesPage>(ctx.http, COMMENT_REPLIES_QUERY, {
        id: comment.id,
        first: REPLIES_PAGE_SIZE,
        cursor: repliesCursor,
      })
      const replies = data.node?.replies
      if (!replies) {
        break
      }
      activities.push(
        ...replies.nodes
          .filter((node): node is DiscussionReplyNode => node !== null)
          .map((reply) => toDiscussionCommentActivity(discussion, reply, true)),
      )
      repliesCursor = replies.pageInfo.hasNextPage ? replies.pageInfo.endCursor : null
    }

    return activities
  }

  const collectDiscussionActivities = async (
    discussion: DiscussionNode,
  ): Promise<GithubActivity[]> => {
    const activities: GithubActivity[] = [toDiscussionStartedActivity(discussion)]

    let commentsCursor: string | null = null
    let hasMore = discussion.comments.totalCount > 0
    while (hasMore) {
      const data = await githubGraphql<DiscussionCommentsPage>(
        ctx.http,
        DISCUSSION_COMMENTS_QUERY,
        {
          id: discussion.id,
          first: COMMENTS_PAGE_SIZE,
          cursor: commentsCursor,
          repliesFirst: REPLIES_PAGE_SIZE,
        },
      )
      const comments = data.node?.comments
      if (!comments) {
        break
      }
      for (const comment of comments.nodes.filter(
        (node): node is DiscussionCommentNode => node !== null,
      )) {
        activities.push(toDiscussionCommentActivity(discussion, comment, Boolean(comment.replyTo)))
        activities.push(...(await collectReplies(discussion, comment)))
      }
      hasMore = comments.pageInfo.hasNextPage
      commentsCursor = comments.pageInfo.endCursor
    }

    return activities
  }

  // discussions has no filterBy.since; walk UPDATED_AT DESC and stop at the watermark,
  // committing only after the walk so a partial run cannot skip older updates.
  let firstPage = true
  let walked = false

  while (ctx.hasRunBudget()) {
    const data = await githubGraphql<DiscussionsPage>(ctx.http, DISCUSSIONS_QUERY, {
      owner,
      repo,
      first: PAGE_SIZE,
      cursor,
    })

    const { pageInfo, nodes } = data.repository.discussions
    const discussions = nodes.filter((node): node is DiscussionNode => node !== null)
    const fresh = sinceDate
      ? discussions.filter((discussion) => new Date(discussion.updatedAt) >= sinceDate)
      : discussions

    if (firstPage && discussions.length > 0) {
      newestUpdatedAt = discussions[0].updatedAt
    }
    firstPage = false

    if (fresh.length > 0) {
      const batches = await mapWithConcurrency(
        fresh,
        ITEM_FETCH_CONCURRENCY,
        collectDiscussionActivities,
      )
      await ctx.emit(batches.flat())
    }

    const reachedWatermark = fresh.length < discussions.length
    cursor = pageInfo.hasNextPage ? pageInfo.endCursor : null
    if (reachedWatermark || !pageInfo.hasNextPage) {
      walked = true
      break
    }
  }

  if (!walked) {
    return { complete: false }
  }

  await ctx.commitWatermark({ phase: 'incremental', since: newestUpdatedAt, cursor: null })
  return { complete: true }
}

export const discussionsSync: SyncDefinition = {
  name: 'discussions',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runDiscussionsSync,
}
