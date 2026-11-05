import { continueAsNew, proxyActivities, sleep } from '@temporalio/workflow'

import * as activities from '../../activities'

const { resolveGitAuthorsPage, syncMember } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 3 },
})

const PAGES_PER_EXECUTION = 500

export async function resolveGitAuthorsToGithub(afterEmail: string | null = null): Promise<void> {
  for (let page = 0; page < PAGES_PER_EXECUTION; page++) {
    const { nextEmail, lookups, rateLimited, memberIdsToSync } =
      await resolveGitAuthorsPage(afterEmail)

    for (const memberId of memberIdsToSync) {
      await syncMember(memberId)
    }

    if (rateLimited) {
      await sleep('15 minutes')
      continue
    }
    if (!nextEmail) {
      return
    }

    afterEmail = nextEmail
    // The GitHub App token is shared with other workers, so lookups are paced to about one a second.
    if (lookups > 0) {
      await sleep(lookups * 1000)
    }
  }

  await continueAsNew<typeof resolveGitAuthorsToGithub>(afterEmail)
}
