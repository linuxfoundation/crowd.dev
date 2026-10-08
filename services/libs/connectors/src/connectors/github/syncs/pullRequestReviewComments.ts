import type { Logger } from '@crowd/logging'

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
const CONNECTION_MAX_ATTEMPTS = 2

interface ConnectionPage<TNode> {
  totalCount: number
  pageInfo: {
    endCursor: string | null
    hasNextPage: boolean
  }
  edges: ({ node: TNode | null } | null)[]
}

interface ConnectionSpec<TPage, TNode> {
  query: string
  select: (page: TPage) => ConnectionPage<TNode> | null | undefined
  missingNode: string
  missingConnection: string
  stillMissingConnection: string
}

const THREADS_CONNECTION: ConnectionSpec<ReviewThreadsBatchPage, ReviewThreadNode> = {
  query: REVIEW_THREADS_FOR_PRS_QUERY,
  select: (page) => page.nodes[0]?.reviewThreads,
  missingNode: 'github returned no pull request node for review threads',
  missingConnection: 'github returned no review threads connection for pull request, retrying',
  stillMissingConnection:
    'github still returned no review threads connection for pull request after retry',
}

const COMMENTS_CONNECTION: ConnectionSpec<ThreadCommentsBatchPage, ThreadCommentNode> = {
  query: COMMENTS_FOR_THREADS_QUERY,
  select: (page) => page.nodes[0]?.comments,
  missingNode: 'github returned no review thread node for comments',
  missingConnection: 'github returned no comments connection for review thread, retrying',
  stillMissingConnection:
    'github still returned no comments connection for review thread after retry',
}

async function fetchConnectionPage<TPage extends { nodes: unknown[] }, TNode>(
  ctx: SyncContext,
  spec: ConnectionSpec<TPage, TNode>,
  variables: Record<string, unknown>,
  log: Logger,
): Promise<ConnectionPage<TNode> | null> {
  for (let attempt = 1; attempt <= CONNECTION_MAX_ATTEMPTS; attempt++) {
    const page = await githubGraphql<TPage>(ctx.http, spec.query, variables, log)
    if (!page.nodes[0]) {
      log.warn(spec.missingNode)
      return null
    }
    const connection = spec.select(page)
    if (connection?.edges) {
      return connection
    }
    log.warn(
      attempt < CONNECTION_MAX_ATTEMPTS ? spec.missingConnection : spec.stillMissingConnection,
    )
  }
  return null
}

async function fetchThreads(
  ctx: SyncContext,
  prId: string,
  prNumber: number,
): Promise<ReviewThreadNode[]> {
  const threads: ReviewThreadNode[] = []
  let cursor: string | null = null
  let expectedCount: number | null = null
  do {
    const log = ctx.log.child({ prNumber, prId, cursor })
    const connection: ConnectionPage<ReviewThreadNode> | null = await fetchConnectionPage(
      ctx,
      THREADS_CONNECTION,
      { ids: [prId], first: THREADS_PAGE_SIZE, after: cursor },
      log,
    )
    if (!connection) {
      break
    }
    threads.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is ReviewThreadNode => Boolean(node?.id)),
    )
    expectedCount = connection.totalCount
    cursor = connection.pageInfo.hasNextPage ? connection.pageInfo.endCursor : null
  } while (cursor)
  if (expectedCount !== null && threads.length < expectedCount) {
    ctx.log.warn(
      { prNumber, prId, expectedCount, fetchedCount: threads.length },
      'fetched fewer review threads than github reports for pull request',
    )
  }
  return threads
}

async function fetchThreadComments(
  ctx: SyncContext,
  threadId: string,
  prNumber: number,
): Promise<ThreadCommentNode[]> {
  const comments: ThreadCommentNode[] = []
  let cursor: string | null = null
  let expectedCount: number | null = null
  do {
    const log = ctx.log.child({ prNumber, threadId, cursor })
    const connection: ConnectionPage<ThreadCommentNode> | null = await fetchConnectionPage(
      ctx,
      COMMENTS_CONNECTION,
      { ids: [threadId], first: COMMENTS_PAGE_SIZE, after: cursor },
      log,
    )
    if (!connection) {
      break
    }
    comments.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is ThreadCommentNode => Boolean(node?.id)),
    )
    expectedCount = connection.totalCount
    cursor = connection.pageInfo.hasNextPage ? connection.pageInfo.endCursor : null
  } while (cursor)
  if (expectedCount !== null && comments.length < expectedCount) {
    ctx.log.warn(
      { prNumber, threadId, expectedCount, fetchedCount: comments.length },
      'fetched fewer review thread comments than github reports for review thread',
    )
  }
  return comments
}

async function runPullRequestReviewCommentsSync(ctx: SyncContext): Promise<SyncOutcome> {
  return runDualPhasePrSync(ctx, async (prs) => {
    const threadsPerPr = await mapWithConcurrency(prs, ITEM_FETCH_CONCURRENCY, (pr) =>
      fetchThreads(ctx, pr.id, pr.number),
    )
    const threads = prs.flatMap((pullRequest, index) =>
      threadsPerPr[index].map((thread) => ({ thread, pullRequest })),
    )

    const commentsPerThread = await mapWithConcurrency(threads, ITEM_FETCH_CONCURRENCY, (entry) =>
      fetchThreadComments(ctx, entry.thread.id, entry.pullRequest.number),
    )

    const activities = threads.flatMap(({ thread, pullRequest }, index) =>
      commentsPerThread[index].map((comment) =>
        toReviewThreadComment(comment, thread, pullRequest),
      ),
    )
    if (activities.length > 0) {
      await ctx.emit(activities)
    }
  })
}

export const pullRequestReviewCommentsSync: SyncDefinition = {
  name: 'pull-request-review-comments',
  cadenceMinutes: 720,
  schema: githubActivitySchema,
  run: runPullRequestReviewCommentsSync,
}
