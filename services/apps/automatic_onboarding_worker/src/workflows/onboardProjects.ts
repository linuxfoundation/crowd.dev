import { log, proxyActivities, rootCause } from '@temporalio/workflow'

import type * as activities from '../activities'
import type { IOnboardProjectsInput } from '../types'

// Short timeout: just a DB read.
const fetchActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// Each onboarding call chains a segment create/query plus GitHub enrichment and integration calls,
// each of which can individually approach a ~30s backend timeout; give generous headroom per project.
const onboardActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 2 },
})

const failureActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 2 },
})

export async function onboardProjects(input: IOnboardProjectsInput = {}): Promise<void> {
  const { batchSize = 20 } = input

  log.info('onboardProjects workflow started.')

  const projects = await fetchActivities.fetchProjectsPendingOnboarding(batchSize)

  if (projects.length === 0) {
    log.info('No projects pending onboarding. Nothing to do.')
    return
  }

  log.info(`Onboarding ${projects.length} project(s) (batch size: ${batchSize}).`)

  let succeeded = 0
  let failed = 0

  for (let i = 0; i < projects.length; i++) {
    const project = projects[i]
    log.info(`[${i + 1}/${projects.length}] Onboarding: ${project.repoUrl}`)

    try {
      await onboardActivities.onboardAndUpdateProject(project)
      succeeded++
    } catch (err) {
      // Activity-level retries are already exhausted at this point — mark as a
      // terminal error so the daily schedule stops retrying this project forever.
      failed++
      const reason = rootCause(err) ?? String(err)
      log.error(
        `Onboarding failed for project id=${project.id} repoUrl=${project.repoUrl}: ${reason}`,
      )

      try {
        await failureActivities.markProjectOnboardingFailed(project.id, reason)
      } catch (markErr) {
        // Don't let a failure to record the error state abort the rest of the batch.
        log.error(`Failed to mark project id=${project.id} as errored: ${String(markErr)}`)
      }
    }
  }

  log.info(
    `Batch onboarding complete. total=${projects.length} succeeded=${succeeded} failed=${failed}`,
  )
}
