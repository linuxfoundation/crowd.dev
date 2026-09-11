import { log, proxyActivities, workflowInfo } from '@temporalio/workflow'

import * as activities from '../activities'

const { findReposForStarSnapshot, getGithubTokenForConnection, fetchAndSaveStarSnapshot } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: '2 minutes',
    retry: { maximumAttempts: 3, backoffCoefficient: 2 },
  })

const CONCURRENCY = 10

export async function captureStarSnapshots(): Promise<void> {
  const capturedAt = workflowInfo().startTime.toISOString()
  const repos = await findReposForStarSnapshot()

  let succeeded = 0
  let failed = 0

  for (let i = 0; i < repos.length; i += CONCURRENCY) {
    const batch = repos.slice(i, i + CONCURRENCY)
    const results = await Promise.allSettled(
      batch.map(async (repo) => {
        const token = await getGithubTokenForConnection(repo.connectionId)
        await fetchAndSaveStarSnapshot(repo.repoUrl, repo.repositoryId, token, capturedAt)
      }),
    )

    for (const [idx, result] of results.entries()) {
      if (result.status === 'rejected') {
        failed++
        log.warn('Failed to capture star snapshot', {
          repoUrl: batch[idx].repoUrl,
          error: (result.reason as Error)?.message ?? result.reason,
        })
      } else {
        succeeded++
      }
    }
  }

  log.info('captureStarSnapshots complete', { total: repos.length, succeeded, failed })
}
