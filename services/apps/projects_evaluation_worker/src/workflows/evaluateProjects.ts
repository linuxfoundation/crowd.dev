import { log, proxyActivities, workflowInfo } from '@temporalio/workflow'

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

// Each AI evaluation call takes ~30-40s; give generous headroom per project.
const evaluateActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '3 minutes',
  retry: { maximumAttempts: 2 },
})

// Quick DB writes — bookkeeping for the run itself.
const pipelineRunActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
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
  const usage = models[model] ?? { calls: 0, inputTokens: 0, outputTokens: 0, costUsd: null }

  usage.calls++
  usage.inputTokens += inputTokens
  usage.outputTokens += outputTokens
  usage.costUsd = costUsd === null ? usage.costUsd : (usage.costUsd ?? 0) + costUsd

  models[model] = usage
}

export async function evaluateProjects(input: IEvaluateProjectsInput = {}): Promise<void> {
  const { batchSize = 50, priorityConfig = DEFAULT_PRIORITY_CONFIG } = input
  const { workflowId, runId } = workflowInfo()

  log.info('evaluateProjects workflow started.')

  const pipelineRunId = await pipelineRunActivities.startEvaluationPipelineRun(workflowId, runId)

  let succeeded = 0
  let failed = 0
  let skipped = 0
  let evaluatorSeconds = 0
  const evaluatorModels: Record<string, IPipelineRunEvaluatorModelUsage> = {}

  try {
    // Step 1: promote 'auto' projects to 'evaluate' according to priority config.
    await promotionActivities.promoteProjectsForEvaluation(priorityConfig)

    // Step 2: fetch the evaluation queue (includes any leftovers from prior runs).
    const projects = await fetchActivities.fetchPendingProjects(batchSize)

    if (projects.length > 0) {
      log.info(`Evaluating ${projects.length} project(s) (batch size: ${batchSize}).`)

      for (let i = 0; i < projects.length; i++) {
        const project = projects[i]
        log.info(`[${i + 1}/${projects.length}] Evaluating: ${project.repoUrl}`)

        try {
          const result = await evaluateActivities.evaluateAndUpdateProject(project)

          if (result === null) {
            skipped++
          } else {
            succeeded++

            if (result.model && result.inputTokens !== null && result.outputTokens !== null) {
              evaluatorSeconds += result.seconds ?? 0
              recordEvaluatorUsage(
                evaluatorModels,
                result.model,
                result.inputTokens,
                result.outputTokens,
                result.costUsd,
              )
            }
          }
        } catch (err) {
          // Log and continue — a single failure should not abort the whole batch.
          failed++
          log.error(
            `Evaluation failed for project id=${project.id} repoUrl=${project.repoUrl}: ${String(err)}`,
          )
        }
      }

      log.info(
        `Batch evaluation complete. total=${projects.length} succeeded=${succeeded} failed=${failed} skipped=${skipped}`,
      )
    } else {
      log.info('No projects pending evaluation. Nothing to do.')
    }

    const modelUsages = Object.values(evaluatorModels)
    const evaluator =
      modelUsages.length > 0
        ? {
            calls: modelUsages.reduce((sum, usage) => sum + usage.calls, 0),
            inputTokens: modelUsages.reduce((sum, usage) => sum + usage.inputTokens, 0),
            outputTokens: modelUsages.reduce((sum, usage) => sum + usage.outputTokens, 0),
            costUsd: modelUsages.reduce(
              (sum, usage) => (usage.costUsd === null ? sum : (sum ?? 0) + usage.costUsd),
              null as number | null,
            ),
            seconds: evaluatorSeconds,
            models: evaluatorModels,
          }
        : null

    await pipelineRunActivities.finishEvaluationPipelineRun(pipelineRunId, {
      status: 'completed',
      totalCandidates: projects.length,
      succeeded,
      failed,
      skipped,
      evaluator,
    })
  } catch (err) {
    await pipelineRunActivities.finishEvaluationPipelineRun(pipelineRunId, {
      status: 'failed',
      succeeded,
      failed,
      skipped,
      errorMessage: String(err),
    })
    throw err
  }
}
