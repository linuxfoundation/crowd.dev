import { mapWithConcurrency } from '../../../concurrency'
import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { PrTimelineItem, PrTimelinePage, PullRequestNode } from '../graphql/pullRequests'
import { PR_TIMELINE_QUERY } from '../graphql/pullRequests'
import { toPullRequestActivities } from '../mappers/pullRequest'
import { ITEM_FETCH_CONCURRENCY } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const TIMELINE_PAGE_SIZE = 50

async function fetchTimeline(ctx: SyncContext, prId: string): Promise<(PrTimelineItem | null)[]> {
  const items: (PrTimelineItem | null)[] = []
  let cursor: string | null = null
  do {
    const data = await githubGraphql<PrTimelinePage>(ctx.http, PR_TIMELINE_QUERY, {
      ids: [prId],
      first: TIMELINE_PAGE_SIZE,
      after: cursor,
    })
    const timeline = data.nodes[0]?.timelineItems
    if (!timeline) {
      break
    }
    items.push(...timeline.nodes)
    cursor = timeline.pageInfo.hasNextPage ? timeline.pageInfo.endCursor : null
  } while (cursor)
  return items
}

async function emitPullRequests(ctx: SyncContext, pullRequests: PullRequestNode[]): Promise<void> {
  const timelines = await mapWithConcurrency(pullRequests, ITEM_FETCH_CONCURRENCY, (pr) =>
    fetchTimeline(ctx, pr.id),
  )

  pullRequests.forEach((pr, index) => {
    const hasMergedEvent = timelines[index].some((item) => item?.__typename === 'MergedEvent')
    if (pr.state === 'MERGED' && !hasMergedEvent) {
      ctx.log.warn(
        { prId: pr.id, prNumber: pr.number, itemCount: timelines[index].length },
        'merged pr timeline missing MergedEvent',
      )
    }
  })

  await ctx.emit(pullRequests.flatMap((pr, index) => toPullRequestActivities(pr, timelines[index])))
}

async function runPullRequestsSync(ctx: SyncContext): Promise<SyncOutcome> {
  return runDualPhasePrSync(ctx, (prs) => emitPullRequests(ctx, prs))
}

export const pullRequestsSync: SyncDefinition = {
  name: 'pull-requests',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runPullRequestsSync,
}
