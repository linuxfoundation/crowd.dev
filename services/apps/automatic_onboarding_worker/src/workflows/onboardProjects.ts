import {
  CancellationScope,
  isCancellation,
  log,
  proxyActivities,
  rootCause,
  workflowInfo,
} from '@temporalio/workflow'

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

const pipelineRunActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

// A retry after a timeout could re-post an already-delivered, non-idempotent webhook message;
// a missed alert is recoverable, a duplicate one is not (see CM-1791).
const notifyActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
})

export async function onboardProjects(input: IOnboardProjectsInput = {}): Promise<void> {
  const { batchSize = 20 } = input
  const { workflowId, runId } = workflowInfo()

  log.info('onboardProjects workflow started.')

  const pipelineRunId = await CancellationScope.nonCancellable(() =>
    pipelineRunActivities.startOnboardingPipelineRun(workflowId, runId),
  )

  let totalCandidates = 0
  let succeeded = 0
  let skipped = 0
  let racedOut = 0
  let failed = 0

  try {
    const projects = await fetchActivities.fetchProjectsPendingOnboarding(batchSize)
    totalCandidates = projects.length

    if (projects.length > 0) {
      log.info(`Onboarding ${projects.length} project(s) (batch size: ${batchSize}).`)

      for (let i = 0; i < projects.length; i++) {
        const project = projects[i]
        log.info(`[${i + 1}/${projects.length}] Onboarding: ${project.repoUrl}`)

        try {
          const outcome = await onboardActivities.onboardAndUpdateProject(project)
          if (outcome === 'onboarded') {
            succeeded++
            try {
              await notifyActivities.notifyOnboardedHumanRequest(project)
            } catch (notifyErr) {
              // A failed alert must never turn a successful onboarding into a batch failure.
              log.error(
                `Failed to send onboarded-request alert for project id=${project.id}: ${String(notifyErr)}`,
              )
            }
          } else {
            skipped++
            if (outcome === 'catalog-changed') {
              racedOut++
            }
          }
        } catch (err) {
          if (isCancellation(err)) {
            throw err
          }

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
        `Batch onboarding complete. total=${projects.length} succeeded=${succeeded} skipped=${skipped} racedOut=${racedOut} failed=${failed}`,
      )
    } else {
      log.info('No projects pending onboarding. Nothing to do.')
    }

    await pipelineRunActivities.finishOnboardingPipelineRun(pipelineRunId, {
      status: 'completed',
      totalCandidates,
      succeeded,
      failed,
      skipped,
      details: { racedOut },
    })
  } catch (err) {
    await CancellationScope.nonCancellable(() =>
      pipelineRunActivities.finishOnboardingPipelineRun(pipelineRunId, {
        status: 'failed',
        totalCandidates,
        succeeded,
        failed,
        skipped,
        details: { racedOut },
        errorMessage: String(err),
      }),
    )
    throw err
  }
}
