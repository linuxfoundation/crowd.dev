import { continueAsNew, proxyActivities, sleep } from '@temporalio/workflow'

import type { IGitAuthor } from '@crowd/data-access-layer'

import * as activities from '../../activities'

const { getGitAuthorsPage, resolveGitAuthor } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { initialInterval: '1 minute', maximumInterval: '15 minutes', maximumAttempts: 10 },
})

export async function resolveGitAuthorsToGithub(afterEmail: string | null = null): Promise<void> {
  const { authors, nextEmail } = await getGitAuthorsPage(afterEmail)

  for (const author of authors) {
    await resolveWaitingOutRateLimits(author)
    // The GitHub App token is shared with other workers, so lookups are paced to about one a second.
    await sleep('1 second')
  }

  if (nextEmail) {
    await continueAsNew<typeof resolveGitAuthorsToGithub>(nextEmail)
  }
}

async function resolveWaitingOutRateLimits(author: IGitAuthor): Promise<void> {
  let resolution = await resolveGitAuthor(author)
  while (resolution.status === 'rate-limited') {
    await sleep(resolution.waitMs)
    resolution = await resolveGitAuthor(author)
  }
}
