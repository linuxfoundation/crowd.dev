import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/nangoActivities'
import { IDeleteGithubRepoConnectionArgs } from '../types'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minute',
  retry: { maximumAttempts: 10, backoffCoefficient: 2 },
})

export async function deleteGithubRepoConnection(
  args: IDeleteGithubRepoConnectionArgs,
): Promise<void> {
  const { integrationId, providerConfigKey, connectionId, repo } = args

  // Delete nango connection
  await activity.deleteConnection(integrationId, providerConfigKey, connectionId)

  // Delete connection from integration.nango_mapping table
  await activity.removeGithubConnection(integrationId, connectionId)

  // Delete from public.repositories
  await activity.unmapGithubRepo(integrationId, repo)
}
