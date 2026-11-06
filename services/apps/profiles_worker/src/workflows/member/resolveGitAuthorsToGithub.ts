import { continueAsNew, proxyActivities, sleep, workflowInfo } from '@temporalio/workflow'

import * as activities from '../../activities'

const { getGitAuthorsPage, resolveGitAuthor, syncMember } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
})

export async function resolveGitAuthorsToGithub(afterEmail: string | null = null): Promise<void> {
  while (!workflowInfo().continueAsNewSuggested) {
    const { authors, nextEmail } = await getGitAuthorsPage(afterEmail)

    for (const author of authors) {
      let result = await resolveGitAuthor(author)
      while (result === 'rate-limited') {
        await sleep('15 minutes')
        result = await resolveGitAuthor(author)
      }
      if (result === 'identity-added') {
        await syncMember(author.memberId)
      }
      // The GitHub App token is shared with other workers, so lookups are paced to about one a second.
      await sleep('1 second')
    }

    if (!nextEmail) {
      return
    }
    afterEmail = nextEmail
  }

  await continueAsNew<typeof resolveGitAuthorsToGithub>(afterEmail)
}
