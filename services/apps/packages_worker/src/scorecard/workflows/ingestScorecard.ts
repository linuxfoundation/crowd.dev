import { proxyActivities } from '@temporalio/workflow'

import type * as scorecardActivities from '../activities'
import { ScorecardPageCounts, addScorecardCounts, emptyScorecardCounts } from '../types'

const { syncGithubRepos } = proxyActivities<typeof scorecardActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { ingestScorecardPage } = proxyActivities<typeof scorecardActivities>({
  startToCloseTimeout: '15 minutes',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 3, initialInterval: '30 seconds', backoffCoefficient: 2 },
})

const PAGE_SIZE = 500

export async function ingestScorecard(): Promise<ScorecardPageCounts> {
  await syncGithubRepos()

  let totals = emptyScorecardCounts()
  let afterId: string | null = null
  do {
    const page: scorecardActivities.IngestScorecardPageOutput = await ingestScorecardPage({
      afterId,
      limit: PAGE_SIZE,
    })
    totals = addScorecardCounts(totals, page.counts)
    afterId = page.nextAfterId
  } while (afterId !== null)

  return totals
}
