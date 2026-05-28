import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'

const { ingestReposCursor } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 2 },
})

const { ingestPackageReposCursor } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '4 hours',
  retry: { maximumAttempts: 2 },
})

export async function ingestRepos(opts: { runId: string }): Promise<void> {
  await ingestReposCursor({ runId: opts.runId })
  await ingestPackageReposCursor({ runId: opts.runId })
}
