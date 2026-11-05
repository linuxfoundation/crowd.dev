import {
  ApplicationFailure,
  CancellationScope,
  TimeoutFailure,
  isCancellation,
  log,
  patched,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import type { IPipelineRunEvaluatorModelUsage } from '@crowd/data-access-layer/src/project-catalog-pipeline-runs/types'

import type * as activities from '../activities'
import {
  EVALUATION_ATTEMPTS,
  EVALUATION_ATTEMPT_TIMEOUT_MINUTES,
  EVALUATION_RETRY_BACKOFF,
  EVALUATION_RETRY_INITIAL_SECONDS,
  TRANSIENT_EVALUATION_FAILURE,
} from '../evaluator/retry'
import type {
  IClassifyAndRouteResult,
  IEvaluateProjectsInput,
  IImmediateOnboardingResult,
  IPriorityConfig,
} from '../types'

// Quick DB write — promote auto → evaluate.
const promotionActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

// Short timeout: just a DB read.
const fetchActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// One batched repo/owner read, then per row: PCC/segment lookups, an LLM parse when the
// request carries text, and one guarded write. Already-classified rows are skipped on retry.
const classifyActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
  heartbeatTimeout: '5 minutes',
  retry: { maximumAttempts: 2 },
})

// Each AI evaluation call takes ~30-40s; give generous headroom per project.
const evaluateActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: `${EVALUATION_ATTEMPT_TIMEOUT_MINUTES} minutes`,
  retry: {
    maximumAttempts: EVALUATION_ATTEMPTS,
    initialInterval: `${EVALUATION_RETRY_INITIAL_SECONDS} seconds`,
    backoffCoefficient: EVALUATION_RETRY_BACKOFF,
  },
})

const pipelineRunActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

// The plan is recorded in the workflow history before anything is mutated, so a retry of the
// apply step (row deletes, a workflow start with a fixed id) is safe and the result stays stable.
const dispatchActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const markActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

// A retry after a timeout could re-post an already-delivered, non-idempotent webhook message;
// a missed alert is recoverable, a duplicate one is not (see CM-1791).
const notifyActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
})

const DEFAULT_PRIORITY_CONFIG: IPriorityConfig = {
  evaluateLimit: 50,
  sourcePriority: ['manual', 'insights-discussions', 'lf-criticality-score'],
}

function recordEvaluatorUsage(
  models: Record<string, IPipelineRunEvaluatorModelUsage>,
  model: string,
  inputTokens: number,
  outputTokens: number,
  costUsd: number | null,
): void {
  const usage = models[model] ?? { calls: 0, inputTokens: 0, outputTokens: 0, costUsd: 0 }

  usage.calls++
  usage.inputTokens += inputTokens
  usage.outputTokens += outputTokens
  usage.costUsd = usage.costUsd === null || costUsd === null ? null : usage.costUsd + costUsd

  models[model] = usage
}

// ActivityFailure's own message is the generic "Activity task failed"; the reason a
// human wants sits at the bottom of the cause chain.
function rootErrorMessage(err: unknown): string {
  let current = err
  for (;;) {
    const cause = current instanceof Error ? (current as { cause?: unknown }).cause : undefined
    if (!(cause instanceof Error)) {
      break
    }
    current = cause
  }
  return current instanceof Error ? current.message : String(current)
}

function isTransientEvaluationFailure(err: unknown): boolean {
  let current = err
  while (current instanceof Error) {
    if (current instanceof TimeoutFailure) {
      return true
    }
    if (current instanceof ApplicationFailure && current.type === TRANSIENT_EVALUATION_FAILURE) {
      return true
    }
    current = (current as { cause?: unknown }).cause
  }
  return false
}

async function postPendingQuestions(catalogIds?: string[]): Promise<void> {
  const questions = await fetchActivities.fetchPendingQuestions(catalogIds)
  for (const project of questions) {
    try {
      await notifyActivities.askRequester(project)
    } catch (notifyErr) {
      // The row keeps a question without a message ts and is asked again next run.
      log.error(`Failed to ask the requester for project id=${project.id}: ${String(notifyErr)}`)
    }
  }
}

async function postReviewRequests(catalogIds?: string[]): Promise<void> {
  const reviewRequests = await fetchActivities.fetchReviewRequests(catalogIds)
  for (const project of reviewRequests) {
    try {
      await notifyActivities.notifyReviewRequest(project)
    } catch (notifyErr) {
      // The row keeps a null reviewMessageTs and is fetched again next run; the batch must go on.
      log.error(
        `Failed to post the review request for project id=${project.id}: ${String(notifyErr)}`,
      )
    }
  }
}

async function notifyEvaluationError(
  project: { id: string; repoUrl: string } & Parameters<typeof activities.notifyEvaluationError>[0],
  reason: string,
): Promise<void> {
  try {
    await notifyActivities.notifyEvaluationError(project, reason)
  } catch (notifyErr) {
    // A failed alert must never turn a recorded error into a batch failure.
    log.error(
      `Failed to send the evaluation error notice for project id=${project.id}: ${String(notifyErr)}`,
    )
  }
}

export async function evaluateProjects(input: IEvaluateProjectsInput = {}): Promise<void> {
  const { batchSize = 50, priorityConfig = DEFAULT_PRIORITY_CONFIG, catalogIds } = input
  const { workflowId, runId } = workflowInfo()

  log.info('evaluateProjects workflow started.')

  const pipelineRunId = await pipelineRunActivities.startEvaluationPipelineRun(workflowId, runId)

  let totalCandidates = 0
  let succeeded = 0
  let failed = 0
  let deferred = 0
  let skipped = 0
  let skippedPreCheck = 0
  let classification: Pick<IClassifyAndRouteResult, 'routed' | 'breakdown' | 'discarded'> | null =
    null
  let immediateOnboarding: IImmediateOnboardingResult | null = null
  let evaluatorSeconds = 0
  const evaluatorModels: Record<string, IPipelineRunEvaluatorModelUsage> = {}

  const buildEvaluatorSummary = () => {
    const modelUsages = Object.values(evaluatorModels)
    return modelUsages.length > 0
      ? {
          calls: modelUsages.reduce((sum, usage) => sum + usage.calls, 0),
          inputTokens: modelUsages.reduce((sum, usage) => sum + usage.inputTokens, 0),
          outputTokens: modelUsages.reduce((sum, usage) => sum + usage.outputTokens, 0),
          costUsd: modelUsages.some((usage) => usage.costUsd === null)
            ? null
            : modelUsages.reduce((sum, usage) => sum + (usage.costUsd as number), 0),
          seconds: evaluatorSeconds,
          models: evaluatorModels,
        }
      : null
  }

  try {
    if (catalogIds) {
      await promotionActivities.promoteCatalogIdsForEvaluation(catalogIds)
    } else {
      await promotionActivities.promoteProjectsForEvaluation(priorityConfig)
    }

    const projects = await fetchActivities.fetchPendingProjects(batchSize, catalogIds)
    totalCandidates = projects.length

    const routing = await classifyActivities.classifyAndRoutePendingProjects(projects)
    const remainingProjects = routing.remaining
    skippedPreCheck = routing.routed.skip
    classification = {
      routed: routing.routed,
      breakdown: routing.breakdown,
      discarded: routing.discarded,
    }

    for (const { project, reason } of routing.skippedHumanRequests) {
      try {
        await notifyActivities.notifySkippedHumanRequest(project, reason)
      } catch (notifyErr) {
        // A failed alert must never turn a recorded skip into a batch failure.
        log.error(
          `Failed to send skipped-request alert for project id=${project.id}: ${String(notifyErr)}`,
        )
      }
    }

    for (const { project, reason } of routing.erroredHumanRequests) {
      await notifyEvaluationError(project, reason)
    }
    await postPendingQuestions(catalogIds)
    failed += routing.failed

    await postReviewRequests(catalogIds)

    if (remainingProjects.length > 0) {
      log.info(`Evaluating ${remainingProjects.length} project(s) (batch size: ${batchSize}).`)

      for (let i = 0; i < remainingProjects.length; i++) {
        const project = remainingProjects[i]
        log.info(`[${i + 1}/${remainingProjects.length}] Evaluating: ${project.repoUrl}`)

        try {
          const result = await evaluateActivities.evaluateAndUpdateProject(project)

          if (result === null) {
            skipped++
          } else if (result.applied && result.action === 'error') {
            failed++
            await notifyEvaluationError(project, result.evaluationReason ?? '(no reason recorded)')
          } else if (result.applied) {
            succeeded++
            if (result.outcome === 'skip' && !result.shared) {
              try {
                await notifyActivities.notifySkippedHumanRequest(
                  project,
                  result.evaluationReason ?? '(no reason provided)',
                  result.organization ?? null,
                )
              } catch (notifyErr) {
                // A failed alert must never turn a recorded skip into a batch failure.
                log.error(
                  `Failed to send skipped-request alert for project id=${project.id}: ${String(notifyErr)}`,
                )
              }
            }
          } else {
            skipped++
          }

          if (result?.model && result.inputTokens !== null && result.outputTokens !== null) {
            evaluatorSeconds += result.seconds ?? 0
            recordEvaluatorUsage(
              evaluatorModels,
              result.model,
              result.inputTokens,
              result.outputTokens,
              result.costUsd,
            )
          }
        } catch (err) {
          if (isCancellation(err)) {
            throw err
          }

          const reason = rootErrorMessage(err)
          const transient = isTransientEvaluationFailure(err)
          log.error(
            `Evaluation failed for project id=${project.id} repoUrl=${project.repoUrl}: ${reason}`,
          )
          try {
            const recorded = await markActivities.markEvaluationFailed(project, reason, transient)
            if (recorded === 'deferred') {
              deferred++
            } else {
              failed++
              if (recorded === 'failed') {
                await notifyEvaluationError(project, reason)
              }
            }
          } catch (markErr) {
            failed++
            // Don't let a failure to record the error state abort the rest of the batch.
            log.error(`Failed to mark project id=${project.id} as errored: ${String(markErr)}`)
          }
        }
      }

      log.info(
        `Batch evaluation complete. total=${remainingProjects.length} succeeded=${succeeded} failed=${failed} deferred=${deferred} skipped=${skipped} skippedPreCheck=${skippedPreCheck}`,
      )
      // The evaluator can route rows to review too (new projects, undecided verdicts).
      if (patched('issue-803-review-after-evaluation')) {
        await postReviewRequests(catalogIds)
      }
    } else {
      log.info('No projects routed to evaluation. Nothing to do.')
    }

    if (catalogIds) {
      const plan = await dispatchActivities.planImmediateOnboarding(catalogIds)
      immediateOnboarding = await dispatchActivities.applyImmediateOnboarding(plan, workflowId)
    }

    await pipelineRunActivities.finishEvaluationPipelineRun(pipelineRunId, {
      status: 'completed',
      totalCandidates,
      succeeded,
      failed,
      skipped,
      skippedPreCheck,
      details: { classification, immediateOnboarding },
      evaluator: buildEvaluatorSummary(),
    })
  } catch (err) {
    await CancellationScope.nonCancellable(() =>
      pipelineRunActivities.finishEvaluationPipelineRun(pipelineRunId, {
        status: 'failed',
        totalCandidates,
        succeeded,
        failed,
        skipped,
        skippedPreCheck,
        details: { classification, immediateOnboarding },
        evaluator: buildEvaluatorSummary(),
        errorMessage: String(err),
      }),
    )
    throw err
  }
}
