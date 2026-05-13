import { ParentClosePolicy, executeChild, proxyActivities, startChild } from '@temporalio/workflow'

import type * as activities from '../activities/nangoActivities'
import { ISyncGithubIntegrationArguments } from '../types'

import { deleteDuplicateGithubConnection } from './deleteDuplicateGithubConnection'
import { deleteGithubRepoConnection } from './deleteGithubRepoConnection'
import { syncGithubRepo } from './syncGithubRepo'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 hour',
  retry: { maximumAttempts: 20, backoffCoefficient: 2 },
})

export async function syncGithubIntegration(args: ISyncGithubIntegrationArguments): Promise<void> {
  const { integrationId } = args

  const result = await activity.analyzeGithubIntegration(integrationId)

  // Delete connections that are no longer needed - fire and forget (parallel)
  for (const repo of result.reposToDelete) {
    await startChild(deleteGithubRepoConnection, {
      workflowId: `sync-github/${integrationId}/delete-connection/${repo.repo.owner}/${repo.repo.repoName}/${repo.connectionId}`,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      args: [
        {
          integrationId,
          providerConfigKey: result.providerConfigKey,
          connectionId: repo.connectionId,
          repo: repo.repo,
        },
      ],
    })
  }

  // Delete duplicate connections - fire and forget (parallel)
  for (const repo of result.duplicatesToDelete) {
    await startChild(deleteDuplicateGithubConnection, {
      workflowId: `sync-github/${integrationId}/delete-duplicate/${repo.repo.owner}/${repo.repo.repoName}/${repo.connectionId}`,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      args: [
        {
          integrationId,
          providerConfigKey: result.providerConfigKey,
          connectionId: repo.connectionId,
          repo: repo.repo,
        },
      ],
    })
  }

  // Create connections for repos that are not already connected - sequential (rate limiting)
  for (const repo of result.reposToSync) {
    const { skipped } = await executeChild(syncGithubRepo, {
      workflowId: `sync-github/${integrationId}/create-connection/${repo.owner}/${repo.repoName}`,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
      args: [
        {
          integrationId,
          providerConfigKey: result.providerConfigKey,
          repo,
        },
      ],
    })

    if (skipped) {
      await activity.logInfo(
        `Not enough time has passed since last connection! Skipping repo ${repo.owner}/${repo.repoName} from integration ${integrationId}!`,
      )
    }
  }
}
