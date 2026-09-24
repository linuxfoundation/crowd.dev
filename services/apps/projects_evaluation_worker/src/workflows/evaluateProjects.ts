import {
  CancellationScope,
  isCancellation,
  log,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import type { IPipelineRunEvaluatorModelUsage } from '@crowd/data-access-layer/src/project-catalog-pipeline-runs/types'

import type * as activities from '../activities'
import type { IEvaluateProjectsInput, IPriorityConfig } from '../types'

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

// Batch DB reads (repo/owner lookups) plus one guarded write per skip decided.
const precheckActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// Each AI evaluation call takes ~30-40s; give generous headroom per project.
const evaluateActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '3 minutes',
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

export async function evaluateProjects(input: IEvaluateProjectsInput = {}): Promise<void> {
  const { batchSize = 50, priorityConfig = DEFAULT_PRIORITY_CONFIG } = input
  const { workflowId, runId } = workflowInfo()

  log.info('evaluateProjects workflow started.')

  const pipelineRunId = await pipelineRunActivities.startEvaluationPipelineRun(workflowId, runId)

  let totalCandidates = 0
  let succeeded = 0
  let failed = 0
  let skipped = 0
  let skippedPreCheck = 0
  let precheckBreakdown: Record<string, number> = {}
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
    // Step 1: promote 'auto' projects to 'evaluate' according to priority config.
    await promotionActivities.promoteProjectsForEvaluation(priorityConfig)

    // Step 2: fetch the evaluation queue (includes any leftovers from prior runs).
    const projects = await fetchActivities.fetchPendingProjects(batchSize)
    totalCandidates = projects.length

    // Step 3: resolve what's already known from CDP before spending an LLM call on it.
    const precheck = await precheckActivities.precheckPendingProjects(projects)
    const remainingProjects = precheck.remaining
    skippedPreCheck = precheck.skippedPreCheck
    precheckBreakdown = precheck.breakdown

    for (const { project, reason } of precheck.skippedDiscussionRequests) {
      try {
        await notifyActivities.notifySkippedHumanRequest(project, reason)
      } catch (notifyErr) {
        // A failed alert must never turn a recorded skip into a batch failure.
        log.error(
          `Failed to send skipped-request alert for project id=${project.id}: ${String(notifyErr)}`,
        )
      }
    }

    if (remainingProjects.length > 0) {
      log.info(`Evaluating ${remainingProjects.length} project(s) (batch size: ${batchSize}).`)

      for (let i = 0; i < remainingProjects.length; i++) {
        const project = remainingProjects[i]
        log.info(`[${i + 1}/${remainingProjects.length}] Evaluating: ${project.repoUrl}`)

        try {
          const result = await evaluateActivities.evaluateAndUpdateProject(project)

          if (result === null) {
            skipped++
          } else if (result.applied) {
            succeeded++
            if (result.outcome === 'skip') {
              try {
                await notifyActivities.notifySkippedHumanRequest(
                  project,
                  result.evaluationReason ?? '(no reason provided)',
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

          failed++
          log.error(
            `Evaluation failed for project id=${project.id} repoUrl=${project.repoUrl}: ${String(err)}`,
          )
        }
      }

      log.info(
        `Batch evaluation complete. total=${remainingProjects.length} succeeded=${succeeded} failed=${failed} skipped=${skipped} skippedPreCheck=${skippedPreCheck}`,
      )
    } else {
      log.info('No projects pending evaluation after pre-check. Nothing to do.')
    }

    await pipelineRunActivities.finishEvaluationPipelineRun(pipelineRunId, {
      status: 'completed',
      totalCandidates,
      succeeded,
      failed,
      skipped,
      skippedPreCheck,
      details: { precheckBreakdown },
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
        details: { precheckBreakdown },
        evaluator: buildEvaluatorSummary(),
        errorMessage: String(err),
      }),
    )
    throw err
  }
}
