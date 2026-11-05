import {
  CancellationScope,
  isCancellation,
  log,
  patched,
  proxyActivities,
  rootCause,
  workflowInfo,
} from '@temporalio/workflow'

import type { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

import type * as activities from '../activities'
import { OnboardingUnit, groupOnboardingUnits } from '../onboardingUnits'
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
  const { batchSize = 20, catalogIds } = input
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
  const onboardedProjects: IDbProjectCatalog[] = []

  try {
    const projects = await fetchActivities.fetchProjectsPendingOnboarding(batchSize, catalogIds)
    totalCandidates = projects.length

    if (projects.length > 0) {
      log.info(`Onboarding ${projects.length} project(s) (batch size: ${batchSize}).`)

      const units: OnboardingUnit[] = patched('issue-817-organization-onboarding')
        ? groupOnboardingUnits(projects)
        : projects.map((project) => ({ kind: 'single', project }))

      const notifySkipped = async (project: IDbProjectCatalog) => {
        try {
          await notifyActivities.notifySkippedBotRequest(project)
        } catch (notifyErr) {
          // A failed reply must never turn a recorded skip into a batch failure.
          log.error(
            `Failed to send the skipped reply for project id=${project.id}: ${String(notifyErr)}`,
          )
        }
      }

      const markFailed = async (project: IDbProjectCatalog, reason: string): Promise<boolean> => {
        try {
          return await failureActivities.markProjectOnboardingFailed(project.id, reason)
        } catch (markErr) {
          // Don't let a failure to record the error state abort the rest of the batch.
          log.error(`Failed to mark project id=${project.id} as errored: ${String(markErr)}`)
          return false
        }
      }

      const notifyErrored = async (project: IDbProjectCatalog, reason: string) => {
        try {
          await notifyActivities.notifyErroredHumanRequest(project, reason)
        } catch (notifyErr) {
          // A failed alert must never turn a recorded error into a batch failure.
          log.error(
            `Failed to send errored-request alert for project id=${project.id}: ${String(notifyErr)}`,
          )
        }
      }

      // The organization card closes only once every member has left 'onboard', so all rows
      // are marked before the one alert goes out.
      const recordFailures = async (members: IDbProjectCatalog[], reason: string) => {
        const marked: IDbProjectCatalog[] = []
        for (const project of members) {
          if (await markFailed(project, reason)) {
            marked.push(project)
          }
        }
        if (marked.length > 0) {
          await notifyErrored(marked[0], reason)
        }
      }

      for (let i = 0; i < units.length; i++) {
        const unit = units[i]
        let pending = unit.kind === 'single' ? [unit.project] : unit.projects
        log.info(
          `[${i + 1}/${units.length}] Onboarding: ${pending.map((project) => project.repoUrl).join(', ')}`,
        )

        try {
          if (unit.kind === 'organization') {
            const skips = await onboardActivities.skipRowsOfDeletedProjects(unit.projects)
            pending = skips.remaining
            skipped += skips.skipped.length
            for (const project of skips.skipped) {
              await notifySkipped(project)
            }
            if (pending.length === 0) {
              continue
            }
            const outcome = await onboardActivities.onboardAndUpdateOrganization(pending)
            succeeded += outcome.onboarded.length
            onboardedProjects.push(...outcome.onboarded)
            skipped += outcome.alreadyHandled + outcome.racedOut
            racedOut += outcome.racedOut
            continue
          }

          const outcome = await onboardActivities.onboardAndUpdateProject(unit.project)
          if (outcome === 'onboarded') {
            succeeded++
            onboardedProjects.push(unit.project)
          } else {
            skipped++
            if (outcome === 'catalog-changed') {
              racedOut++
            }
            if (outcome === 'skipped') {
              await notifySkipped(unit.project)
            }
          }
        } catch (err) {
          if (isCancellation(err)) {
            throw err
          }

          // Activity-level retries are already exhausted at this point — mark as a
          // terminal error so the daily schedule stops retrying this project forever.
          failed += pending.length
          const reason = rootCause(err) ?? String(err)
          log.error(
            `Onboarding failed for ${pending.map((project) => `id=${project.id} repoUrl=${project.repoUrl}`).join(', ')}: ${reason}`,
          )
          await recordFailures(pending, reason)
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
  } finally {
    await CancellationScope.nonCancellable(async () => {
      try {
        await notifyActivities.notifyOnboardedHumanRequests(onboardedProjects)
      } catch (notifyErr) {
        // A failed alert must never turn a successful onboarding into a batch failure.
        log.error(`Failed to send onboarded-request alerts: ${String(notifyErr)}`)
      }
    })
  }
}
