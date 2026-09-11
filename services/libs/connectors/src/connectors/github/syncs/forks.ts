import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { ForkNode, ForksPage } from '../graphql/forks'
import { FORKS_QUERY } from '../graphql/forks'
import { toFork } from '../mappers/fork'
import { PAGE_SIZE, parseRepoChannel, readWatermark } from '../paging'
import { githubActivitySchema } from '../schemas'

async function runForksSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  let since = watermark.since
  let cursor = watermark.cursor

  while (ctx.hasRunBudget()) {
    const data = await githubGraphql<ForksPage>(ctx.http, FORKS_QUERY, {
      owner,
      repo,
      first: PAGE_SIZE,
      cursor,
    })

    const { pageInfo, nodes } = data.repository.forks
    const forks = nodes
      .filter((node): node is ForkNode => node !== null)
      .filter((node) => !since || node.createdAt > since)

    if (forks.length > 0) {
      await ctx.emit(forks.map(toFork))
      since = forks[forks.length - 1].createdAt
    }

    cursor = pageInfo.endCursor ?? cursor
    await ctx.commitWatermark({ phase: 'incremental', since, cursor })

    if (!pageInfo.hasNextPage) {
      return { complete: true }
    }
  }

  return { complete: false }
}

export const forksSync: SyncDefinition = {
  name: 'forks',
  cadenceMinutes: 360,
  schema: githubActivitySchema,
  run: runForksSync,
}
