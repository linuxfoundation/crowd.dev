import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/nangoActivities'
import { ISyncGithubRepoArgs, ISyncGithubRepoResult } from '../types'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 hour',
  retry: { maximumAttempts: 20, backoffCoefficient: 2 },
})

export async function syncGithubRepo(args: ISyncGithubRepoArgs): Promise<ISyncGithubRepoResult> {
  const { integrationId, providerConfigKey, repo } = args

  // Check if we can create a connection (rate limiting)
  const canCreate = await activity.canCreateGithubConnection()

  if (!canCreate) {
    return { skipped: true }
  }

  // Create nango connection
  const connectionId = await activity.createGithubConnection(integrationId, repo)

  // Add connection to integration.nango_mapping table
  await activity.setGithubConnection(integrationId, repo, connectionId)

  // Add repo to git integration
  await activity.updateGitIntegrationWithRepo(integrationId, repo)

  // Add repo to public.repositories
  await activity.mapGithubRepoToRepositories(integrationId, repo)

  // Link nango_mapping row to the newly created repository
  await activity.linkNangoMappingToRepo(integrationId, connectionId, repo)

  // Start nango sync
  await activity.startNangoSync(integrationId, providerConfigKey, connectionId)

  return { skipped: false }
}
