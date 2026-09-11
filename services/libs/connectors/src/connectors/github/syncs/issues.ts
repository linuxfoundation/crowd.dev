import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { IssueNode, IssuesPage } from '../graphql/issues'
import { ISSUES_QUERY } from '../graphql/issues'
import { toIssueActivities } from '../mappers/issue'
import { PAGE_SIZE, parseRepoChannel, readWatermark } from '../paging'
import { githubActivitySchema } from '../schemas'

async function runIssuesSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  // filterBy.since is coupled to the cursor's result set: advancing it between
  // pages would invalidate cursors, so the query keeps the run-start since.
  const querySince = watermark.since
  let since = watermark.since
  let cursor: string | null = null

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

    if (issues.length > 0) {
      await ctx.emit(issues.flatMap(toIssueActivities))
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

export const issuesSync: SyncDefinition = {
  name: 'issues',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runIssuesSync,
}
