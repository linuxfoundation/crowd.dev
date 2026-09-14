import { QueryExecutor } from '../queryExecutor'
import { prepareSelectColumns } from '../utils'

import { IDbPipelineRun, IPipelineRunFilter, IPipelineRunFinish, IPipelineRunStart } from './types'

const PIPELINE_RUN_COLUMNS = [
  'id',
  'stage',
  'status',
  'workflowId',
  'temporalRunId',
  'startedAt',
  'finishedAt',
  'elapsedSeconds',
  'totalCandidates',
  'succeeded',
  'failed',
  'skipped',
  'skippedPreCheck',
  'errorMessage',
  'details',
  'evaluatorCalls',
  'evaluatorInputTokens',
  'evaluatorOutputTokens',
  'evaluatorCostUsd',
  'evaluatorSeconds',
  'evaluatorModels',
  'createdAt',
  'updatedAt',
]

export async function startPipelineRun(
  qx: QueryExecutor,
  data: IPipelineRunStart,
): Promise<IDbPipelineRun> {
  return qx.selectOne(
    `
    INSERT INTO "projectCatalogPipelineRuns" (
      "stage",
      "workflowId",
      "temporalRunId",
      "startedAt",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(stage),
      $(workflowId),
      $(temporalRunId),
      NOW(),
      NOW(),
      NOW()
    )
    RETURNING ${prepareSelectColumns(PIPELINE_RUN_COLUMNS)}
    `,
    {
      stage: data.stage,
      workflowId: data.workflowId ?? null,
      temporalRunId: data.temporalRunId ?? null,
    },
  )
}

// Guarded by "finishedAt" IS NULL so a run can only be finalized once — same
// pattern as finalizeProjectCatalogEvaluation in project-catalog.ts.
export async function finishPipelineRun(
  qx: QueryExecutor,
  id: string,
  data: IPipelineRunFinish,
): Promise<IDbPipelineRun | null> {
  const evaluator = data.evaluator ?? null

  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalogPipelineRuns"
    SET
      "status" = $(status),
      "totalCandidates" = $(totalCandidates),
      "succeeded" = $(succeeded),
      "failed" = $(failed),
      "skipped" = $(skipped),
      "skippedPreCheck" = $(skippedPreCheck),
      "errorMessage" = $(errorMessage),
      "details" = $(details),
      "evaluatorCalls" = $(evaluatorCalls),
      "evaluatorInputTokens" = $(evaluatorInputTokens),
      "evaluatorOutputTokens" = $(evaluatorOutputTokens),
      "evaluatorCostUsd" = $(evaluatorCostUsd),
      "evaluatorSeconds" = $(evaluatorSeconds),
      "evaluatorModels" = $(evaluatorModels),
      "finishedAt" = NOW(),
      "elapsedSeconds" = EXTRACT(EPOCH FROM (NOW() - "startedAt")),
      "updatedAt" = NOW()
    WHERE id = $(id) AND "finishedAt" IS NULL
    RETURNING ${prepareSelectColumns(PIPELINE_RUN_COLUMNS)}
    `,
    {
      id,
      status: data.status,
      totalCandidates: data.totalCandidates ?? null,
      succeeded: data.succeeded ?? null,
      failed: data.failed ?? null,
      skipped: data.skipped ?? null,
      skippedPreCheck: data.skippedPreCheck ?? null,
      errorMessage: data.errorMessage ?? null,
      details: data.details ? JSON.stringify(data.details) : null,
      evaluatorCalls: evaluator?.calls ?? null,
      evaluatorInputTokens: evaluator?.inputTokens ?? null,
      evaluatorOutputTokens: evaluator?.outputTokens ?? null,
      evaluatorCostUsd: evaluator?.costUsd ?? null,
      evaluatorSeconds: evaluator?.seconds ?? null,
      evaluatorModels: evaluator?.models ? JSON.stringify(evaluator.models) : null,
    },
  )
}

export async function findLatestPipelineRun(
  qx: QueryExecutor,
  stage: string,
): Promise<IDbPipelineRun | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(PIPELINE_RUN_COLUMNS)}
    FROM "projectCatalogPipelineRuns"
    WHERE "stage" = $(stage)
    ORDER BY "startedAt" DESC
    LIMIT 1
    `,
    { stage },
  )
}

export async function findPipelineRuns(
  qx: QueryExecutor,
  options: IPipelineRunFilter = {},
): Promise<IDbPipelineRun[]> {
  const { stage, from, to, limit, offset } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PIPELINE_RUN_COLUMNS)}
    FROM "projectCatalogPipelineRuns"
    WHERE ($(stage)::text IS NULL OR "stage" = $(stage))
      AND ($(from)::timestamptz IS NULL OR "startedAt" >= $(from))
      AND ($(to)::timestamptz IS NULL OR "startedAt" <= $(to))
    ORDER BY "startedAt" DESC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { stage: stage ?? null, from: from ?? null, to: to ?? null, limit, offset },
  )
}
