import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { StargazerEdge, StargazersPage } from '../graphql/stars'
import { STARGAZERS_QUERY } from '../graphql/stars'
import { toStar } from '../mappers/star'
import { PAGE_SIZE, parseRepoChannel, readWatermark } from '../paging'
import { githubActivitySchema } from '../schemas'

async function runStarsSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  let since = watermark.since
  let cursor = watermark.cursor

  while (ctx.hasRunBudget()) {
    const data = await githubGraphql<StargazersPage>(ctx.http, STARGAZERS_QUERY, {
      owner,
      repo,
      first: PAGE_SIZE,
      cursor,
    })

    const { pageInfo, edges } = data.repository.stargazers
    const stargazers = edges
      .filter((edge): edge is StargazerEdge => edge !== null)
      .filter((edge) => !since || edge.starredAt > since)

    if (stargazers.length > 0) {
      await ctx.emit(stargazers.map(toStar))
      since = stargazers[stargazers.length - 1].starredAt
    }

    cursor = pageInfo.endCursor ?? cursor
    await ctx.commitWatermark({ phase: 'incremental', since, cursor })

    if (!pageInfo.hasNextPage) {
      return { complete: true }
    }
  }

  return { complete: false }
}

export const starsSync: SyncDefinition = {
  name: 'stars',
  cadenceMinutes: 1440,
  schema: githubActivitySchema,
  run: runStarsSync,
}
