import {
  finalizeProjectCatalogEvaluation,
  findProjectCatalogById,
  findProjectCatalogPendingEvaluation,
  finishPipelineRun,
  promoteProjectsToEvaluate,
  startPipelineRun,
} from '@crowd/data-access-layer'
import { IPipelineRunFinish } from '@crowd/data-access-layer/src/project-catalog-pipeline-runs/types'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { estimateLlmCostUsd } from '@crowd/types'

import { evaluateProject } from '../evaluator/evaluator'
import { svc } from '../main'
import { IEvaluationActivityResult, IPriorityConfig } from '../types'

const log = getServiceLogger()

/**
 * Promotes 'auto' projects to 'evaluate' up to the configured limit.
 * Count, slot computation, locking, and update are all done atomically
 * inside a single SQL statement — see promoteProjectsToEvaluate in the DAL.
 */
export async function promoteProjectsForEvaluation(config: IPriorityConfig): Promise<void> {
  const { evaluateLimit, sourcePriority } = config
  const qx = pgpQx(svc.postgres.writer.connection())

  log.info({ evaluateLimit, sourcePriority }, 'Priority promotion: starting.')

  const promoted = await promoteProjectsToEvaluate(qx, { evaluateLimit, sourcePriority })

  log.info({ promoted }, 'Priority promotion: complete.')
}

export async function fetchPendingProjects(batchSize: number): Promise<IDbProjectCatalog[]> {
  const qx = pgpQx(svc.postgres.reader.connection())

  const projects = await findProjectCatalogPendingEvaluation(qx, { limit: batchSize })

  log.info({ count: projects.length, batchSize }, 'Fetched projects pending evaluation.')

  return projects
}

export async function evaluateAndUpdateProject(
  project: IDbProjectCatalog,
): Promise<IEvaluationActivityResult | null> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  // Guard: fetch fresh state to ensure the API is called at most once per project.
  // Uses the writer connection to avoid replica lag missing a just-written evaluatedAt.
  const fresh = await findProjectCatalogById(qx, project.id)
  if (fresh?.evaluatedAt) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, evaluatedAt: fresh.evaluatedAt },
      'Project already evaluated, skipping API call.',
    )
    return null
  }

  log.info({ id: project.id, repoUrl: project.repoUrl }, 'Starting evaluation.')

  const result = await evaluateProject({
    id: project.id,
    repoUrl: project.repoUrl,
    repoName: project.repoName,
    projectSlug: project.projectSlug,
    lfCriticalityScore: project.lfCriticalityScore,
    source: project.source,
  })

  const updated = await finalizeProjectCatalogEvaluation(qx, project.id, {
    action: result.outcome,
    evaluationResult: result.evaluationResult,
    evaluationReason: result.evaluationReason,
  })

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  if (!updated) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, elapsedSeconds },
      'Project was moved out of evaluate by a manual request while evaluating, discarding DB update.',
    )
  } else {
    log.info(
      {
        id: project.id,
        repoUrl: project.repoUrl,
        outcome: result.outcome,
        evaluationResult: result.evaluationResult,
        evaluationReason: result.evaluationReason,
        elapsedSeconds,
      },
      'Evaluation complete.',
    )
  }

  if (!result.metrics) {
    return {
      applied: Boolean(updated),
      outcome: result.outcome,
      model: null,
      inputTokens: null,
      outputTokens: null,
      costUsd: null,
      seconds: null,
    }
  }

  const { model, inputTokens, outputTokens, seconds } = result.metrics

  return {
    applied: Boolean(updated),
    outcome: result.outcome,
    model,
    inputTokens,
    outputTokens,
    costUsd: estimateLlmCostUsd(model, inputTokens, outputTokens),
    seconds,
  }
}

export async function startEvaluationPipelineRun(
  workflowId: string | null,
  temporalRunId: string | null,
): Promise<string> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const run = await startPipelineRun(qx, { stage: 'evaluation', workflowId, temporalRunId })

  return run.id
}

export async function finishEvaluationPipelineRun(
  id: string,
  data: IPipelineRunFinish,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  await finishPipelineRun(qx, id, data)
}
