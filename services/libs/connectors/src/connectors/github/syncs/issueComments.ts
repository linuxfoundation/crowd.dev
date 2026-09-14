import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type {
  IssueCommentNode,
  IssueCommentsBatchPage,
  IssueCommentsPaginatedPage,
  IssueNode,
  IssuesPage,
} from '../graphql/issues'
import {
  ISSUES_QUERY,
  ISSUE_COMMENTS_PAGINATED_QUERY,
  ISSUE_COMMENTS_QUERY,
} from '../graphql/issues'
import { toIssueComment } from '../mappers/issueComment'
import { PAGE_SIZE, parseRepoChannel, readWatermark } from '../paging'
import { githubActivitySchema } from '../schemas'

const ISSUE_BATCH_SIZE = 15
const COMMENTS_BATCH_PAGE_SIZE = 25
// api is highly unreliable when paginating comments per issue, so keep the page small
// (nango workaround: nango-integrations/github/syncs/issue-comments.ts)
const COMMENTS_PAGINATED_PAGE_SIZE = 5

async function runIssueCommentsSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  const querySince = watermark.since
  const sinceDate = querySince ? new Date(querySince) : null
  let since = watermark.since
  let cursor: string | null = null

  const emitComments = async (
    nodes: (IssueCommentNode | null)[],
    issue: IssueNode,
  ): Promise<void> => {
    const comments = nodes
      .filter((node): node is IssueCommentNode => node !== null && Boolean(node.id))
      .filter((node) => !sinceDate || new Date(node.createdAt) >= sinceDate)
    if (comments.length > 0) {
      await ctx.emit(comments.map((comment) => toIssueComment(comment, issue)))
    }
  }

  const drainRemainingComments = async (issue: IssueNode, startCursor: string | null) => {
    let commentsCursor = startCursor
    let hasMore = true
    while (hasMore) {
      const data = await githubGraphql<IssueCommentsPaginatedPage>(
        ctx.http,
        ISSUE_COMMENTS_PAGINATED_QUERY,
        {
          owner,
          repo,
          issueNumber: issue.number,
          first: COMMENTS_PAGINATED_PAGE_SIZE,
          cursor: commentsCursor,
        },
      )
      const comments = data.repository.issue?.comments
      if (!comments?.nodes) {
        return
      }
      await emitComments(comments.nodes, issue)
      hasMore = comments.pageInfo.hasNextPage
      commentsCursor = comments.pageInfo.endCursor
    }
  }

  while (ctx.hasRunBudget()) {
    const data = await githubGraphql<IssuesPage>(ctx.http, ISSUES_QUERY, {
      owner,
      repo,
      first: PAGE_SIZE,
      cursor,
      since: querySince,
    })

    const { pageInfo, nodes } = data.repository.issues
    const issues = nodes.filter((node): node is IssueNode => node !== null)

    for (let i = 0; i < issues.length; i += ISSUE_BATCH_SIZE) {
      const batch = issues.slice(i, i + ISSUE_BATCH_SIZE)
      const batchData = await githubGraphql<IssueCommentsBatchPage>(
        ctx.http,
        ISSUE_COMMENTS_QUERY,
        {
          ids: batch.map((issue) => issue.id),
          first: COMMENTS_BATCH_PAGE_SIZE,
        },
      )

      const toPaginate: { issue: IssueNode; commentsCursor: string | null }[] = []
      for (const node of batchData.nodes) {
        if (!node?.comments?.nodes) {
          continue
        }
        const issue = batch.find((candidate) => candidate.id === node.id)
        if (!issue) {
          continue
        }
        await emitComments(node.comments.nodes, issue)
        if (node.comments.pageInfo.hasNextPage) {
          toPaginate.push({ issue, commentsCursor: node.comments.pageInfo.endCursor })
        }
      }

      for (const { issue, commentsCursor } of toPaginate) {
        await drainRemainingComments(issue, commentsCursor)
      }
    }

    if (issues.length > 0) {
      since = issues[issues.length - 1].updatedAt
    }
    cursor = pageInfo.hasNextPage ? pageInfo.endCursor : null
    await ctx.commitWatermark({ phase: 'incremental', since, cursor: null })

    if (!pageInfo.hasNextPage) {
      return { complete: true }
    }
  }

  return { complete: false }
}

export const issueCommentsSync: SyncDefinition = {
  name: 'issue-comments',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runIssueCommentsSync,
}
