import { proxyActivities } from '@temporalio/workflow'

import * as activities from '../../activities'

const { getResolveGitAuthorsCursor, saveResolveGitAuthorsCursor, resolveGitAuthorsPage } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: '30 minutes',
    retry: { maximumAttempts: 3 },
  })

// The GitHub App token is shared with other workers, and pages with nothing left to look up
// still grow the workflow history, so each run is capped on both.
const MAX_LOOKUPS_PER_RUN = 10_000
const MAX_PAGES_PER_RUN = 1000

export async function resolveGitAuthorsToGithub(): Promise<void> {
  let afterEmail = await getResolveGitAuthorsCursor()
  let lookups = 0

  for (let page = 0; page < MAX_PAGES_PER_RUN && lookups < MAX_LOOKUPS_PER_RUN; page++) {
    const result = await resolveGitAuthorsPage(afterEmail)
    lookups += result.lookups
    afterEmail = result.nextEmail

    if (result.rateLimited || !afterEmail) {
      break
    }
  }

  await saveResolveGitAuthorsCursor(afterEmail)
}
