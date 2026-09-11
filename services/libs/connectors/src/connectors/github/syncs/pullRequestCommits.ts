import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { PrCommitNode, PrCommitsPage } from '../graphql/pullRequestChildren'
import { PR_COMMITS_QUERY } from '../graphql/pullRequestChildren'
import { toCommit } from '../mappers/commit'
import { parseRepoChannel } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const COMMITS_PAGE_SIZE = 50

async function runPullRequestCommitsSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)

  return runDualPhasePrSync(ctx, async (prs) => {
    for (const pullRequest of prs) {
      let cursor: string | null = null
      let hasMore = true

      while (hasMore) {
        const data = await githubGraphql<PrCommitsPage>(ctx.http, PR_COMMITS_QUERY, {
          owner,
          repo,
          prNumber: pullRequest.number,
          first: COMMITS_PAGE_SIZE,
          cursor,
        })

        const commits = data.repository.pullRequest?.commits
        if (!commits) {
          break
        }

        const fresh = commits.nodes
          .map((node) => node?.commit)
          .filter((commit): commit is PrCommitNode['commit'] =>
            Boolean(commit?.author?.user?.login),
          )

        if (fresh.length > 0) {
          await ctx.emit(fresh.map((commit) => toCommit(commit, pullRequest.id)))
        }

        hasMore = commits.pageInfo.hasNextPage
        cursor = commits.pageInfo.endCursor
      }
    }
  })
}

export const pullRequestCommitsSync: SyncDefinition = {
  name: 'pull-request-commits',
  cadenceMinutes: 120,
  schema: githubActivitySchema,
  run: runPullRequestCommitsSync,
}
