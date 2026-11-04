import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/githubLifecycleActivities'
import type { IOwnedGithubRename } from '../activities/githubLifecycleActivities'
import type { IGithubRenamesResult } from '../githubRenames'

const { applyRenames } = proxyActivities<Pick<typeof activities, 'applyRenames'>>({
  startToCloseTimeout: '30 minutes',
  heartbeatTimeout: '5 minutes',
  retry: {
    maximumAttempts: 1,
  },
})

export async function applyGithubRename(input: IOwnedGithubRename): Promise<IGithubRenamesResult> {
  return applyRenames({ renames: [input] })
}
