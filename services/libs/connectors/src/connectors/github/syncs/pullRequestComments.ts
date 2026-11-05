import { mapWithConcurrency } from '../../../concurrency'
import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import type { PrCommentNode, PrCommentsBatchPage } from '../graphql/pullRequestChildren'
import { COMMENTS_FOR_PRS_QUERY } from '../graphql/pullRequestChildren'
import { toPrComment } from '../mappers/prComment'
import { ITEM_FETCH_CONCURRENCY } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'
import type { ConnectionPage, ConnectionSpec } from './connectionPage'
import { fetchConnectionPage } from './connectionPage'

const COMMENTS_PAGE_SIZE = 50

const COMMENTS_CONNECTION: ConnectionSpec<PrCommentsBatchPage, PrCommentNode> = {
  query: COMMENTS_FOR_PRS_QUERY,
  select: (page) => page.nodes[0]?.comments,
  missingNode: 'github returned no pull request node for comments',
  missingConnection: 'github returned no comments connection for pull request, retrying',
  stillMissingConnection:
    'github still returned no comments connection for pull request after retry',
}

async function fetchComments(
  ctx: SyncContext,
  prId: string,
  prNumber: number,
): Promise<PrCommentNode[]> {
  const comments: PrCommentNode[] = []
  let cursor: string | null = null
  let expectedCount: number | null = null
  do {
    const log = ctx.log.child({ prNumber, prId, cursor })
    const connection: ConnectionPage<PrCommentNode> | null = await fetchConnectionPage(
      ctx,
      COMMENTS_CONNECTION,
      { ids: [prId], first: COMMENTS_PAGE_SIZE, after: cursor },
      log,
    )
    if (!connection) {
      break
    }
    comments.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is PrCommentNode => Boolean(node?.id)),
    )
    expectedCount = connection.totalCount
    cursor = connection.pageInfo.hasNextPage ? connection.pageInfo.endCursor : null
  } while (cursor)
  if (expectedCount !== null && comments.length < expectedCount) {
    ctx.log.warn(
      { prNumber, prId, expectedCount, fetchedCount: comments.length },
      'fetched fewer comments than github reports for pull request',
    )
  }
  return comments
}

async function runPullRequestCommentsSync(ctx: SyncContext): Promise<SyncOutcome> {
  return runDualPhasePrSync(ctx, async (prs) => {
    const commentsPerPr = await mapWithConcurrency(prs, ITEM_FETCH_CONCURRENCY, (pr) =>
      fetchComments(ctx, pr.id, pr.number),
    )
    const activities = prs.flatMap((pullRequest, index) =>
      commentsPerPr[index].map((comment) => toPrComment(comment, pullRequest)),
    )
    if (activities.length > 0) {
      await ctx.emit(activities)
    }
  })
}

export const pullRequestCommentsSync: SyncDefinition = {
  name: 'pull-request-comments',
  cadenceMinutes: 720,
  schema: githubActivitySchema,
  run: runPullRequestCommentsSync,
}
