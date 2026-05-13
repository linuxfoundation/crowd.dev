import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/nangoActivities'
import { IDeleteDuplicateGithubConnectionArgs } from '../types'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minute',
  retry: { maximumAttempts: 10, backoffCoefficient: 2 },
})

export async function deleteDuplicateGithubConnection(
  args: IDeleteDuplicateGithubConnectionArgs,
): Promise<void> {
  const { integrationId, providerConfigKey, connectionId } = args

  // Delete nango connection
  await activity.deleteConnection(integrationId, providerConfigKey, connectionId)

  // Delete connection from integration.nango_mapping table
  await activity.removeGithubConnection(integrationId, connectionId)

  // We don't unmap because this one was duplicated
}
