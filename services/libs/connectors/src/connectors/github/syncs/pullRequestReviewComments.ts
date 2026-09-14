import { mapWithConcurrency } from '../../../concurrency'
import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type {
  ReviewThreadNode,
  ReviewThreadsBatchPage,
  ThreadCommentNode,
  ThreadCommentsBatchPage,
} from '../graphql/pullRequestChildren'
import {
  COMMENTS_FOR_THREADS_QUERY,
  REVIEW_THREADS_FOR_PRS_QUERY,
} from '../graphql/pullRequestChildren'
import { toReviewThreadComment } from '../mappers/reviewThreadComment'
import { ITEM_FETCH_CONCURRENCY } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const THREADS_PAGE_SIZE = 50
const COMMENTS_PAGE_SIZE = 50

async function fetchThreads(ctx: SyncContext, prId: string): Promise<ReviewThreadNode[]> {
  const threads: ReviewThreadNode[] = []
  let cursor: string | null = null
  do {
    const data = await githubGraphql<ReviewThreadsBatchPage>(
      ctx.http,
      REVIEW_THREADS_FOR_PRS_QUERY,
      { ids: [prId], first: THREADS_PAGE_SIZE, after: cursor },
    )
    const connection = data.nodes[0]?.reviewThreads
    if (!connection?.edges) {
      break
    }
    threads.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is ReviewThreadNode => Boolean(node?.id)),
    )
    cursor = connection.pageInfo.hasNextPage ? connection.pageInfo.endCursor : null
  } while (cursor)
  return threads
}

async function fetchThreadComments(
  ctx: SyncContext,
  threadId: string,
): Promise<ThreadCommentNode[]> {
  const comments: ThreadCommentNode[] = []
  let cursor: string | null = null
  do {
    const data = await githubGraphql<ThreadCommentsBatchPage>(
      ctx.http,
      COMMENTS_FOR_THREADS_QUERY,
      { ids: [threadId], first: COMMENTS_PAGE_SIZE, after: cursor },
    )
    const connection = data.nodes[0]?.comments
    if (!connection?.edges) {
      break
    }
    comments.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is ThreadCommentNode => Boolean(node?.id)),
    )
    cursor = connection.pageInfo.hasNextPage ? connection.pageInfo.endCursor : null
  } while (cursor)
  return comments
}

async function runPullRequestReviewCommentsSync(ctx: SyncContext): Promise<SyncOutcome> {
  return runDualPhasePrSync(ctx, async (prs, sinceDate) => {
    const threadsPerPr = await mapWithConcurrency(prs, ITEM_FETCH_CONCURRENCY, (pr) =>
      fetchThreads(ctx, pr.id),
    )
    const threads = prs.flatMap((pullRequest, index) =>
      threadsPerPr[index].map((thread) => ({ thread, pullRequest })),
    )

    const commentsPerThread = await mapWithConcurrency(threads, ITEM_FETCH_CONCURRENCY, (entry) =>
      fetchThreadComments(ctx, entry.thread.id),
    )

    const activities = threads.flatMap(({ thread, pullRequest }, index) =>
      commentsPerThread[index]
        .filter((comment) => !sinceDate || new Date(comment.createdAt) >= sinceDate)
        .map((comment) => toReviewThreadComment(comment, thread, pullRequest)),
    )
    if (activities.length > 0) {
      await ctx.emit(activities)
    }
  })
}

export const pullRequestReviewCommentsSync: SyncDefinition = {
  name: 'pull-request-review-comments',
  cadenceMinutes: 120,
  schema: githubActivitySchema,
  run: runPullRequestReviewCommentsSync,
}
