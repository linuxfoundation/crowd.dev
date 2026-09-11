import { mapWithConcurrency } from '../../../concurrency'
import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { PrCommentNode, PrCommentsBatchPage } from '../graphql/pullRequestChildren'
import { COMMENTS_FOR_PRS_QUERY } from '../graphql/pullRequestChildren'
import { toPrComment } from '../mappers/prComment'
import { ITEM_FETCH_CONCURRENCY } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const COMMENTS_PAGE_SIZE = 50

async function fetchComments(ctx: SyncContext, prId: string): Promise<PrCommentNode[]> {
  const comments: PrCommentNode[] = []
  let cursor: string | null = null
  do {
    const data = await githubGraphql<PrCommentsBatchPage>(ctx.http, COMMENTS_FOR_PRS_QUERY, {
      ids: [prId],
      first: COMMENTS_PAGE_SIZE,
      after: cursor,
    })
    const connection = data.nodes[0]?.comments
    if (!connection?.edges) {
      break
    }
    comments.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is PrCommentNode => Boolean(node?.id)),
    )
    cursor = connection.pageInfo.hasNextPage ? connection.pageInfo.endCursor : null
  } while (cursor)
  return comments
}

async function runPullRequestCommentsSync(ctx: SyncContext): Promise<SyncOutcome> {
  return runDualPhasePrSync(ctx, async (prs, sinceDate) => {
    const commentsPerPr = await mapWithConcurrency(prs, ITEM_FETCH_CONCURRENCY, (pr) =>
      fetchComments(ctx, pr.id),
    )
    const activities = prs.flatMap((pullRequest, index) =>
      commentsPerPr[index]
        .filter((comment) => !sinceDate || new Date(comment.createdAt) >= sinceDate)
        .map((comment) => toPrComment(comment, pullRequest)),
    )
    if (activities.length > 0) {
      await ctx.emit(activities)
    }
  })
}

export const pullRequestCommentsSync: SyncDefinition = {
  name: 'pull-request-comments',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runPullRequestCommentsSync,
}
