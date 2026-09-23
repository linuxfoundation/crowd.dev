import { QueryExecutor } from '../queryExecutor'
import {
  DocReadinessRunTrigger,
  IDbDocReadinessRun,
  IDocReadinessRunFinish,
  IDocReadinessRunStart,
} from './types'

const RUN_COLUMNS = [
  'id',
  'trigger',
  'scope',
  'status',
  'workflowId',
  'temporalRunId',
  'startedAt',
  'finishedAt',
  'totalProjects',
  'discovered',
  'scored',
  'failed',
  'errorMessage',
  'createdAt',
  'updatedAt',
]
  .map((c) => `"${c}"`)
  .join(',\n')

// Upserts on temporalRunId, not workflowId, since a retry reuses the workflow
// id but gets a new run id — keying on workflowId would collapse both runs.
export async function startDocReadinessRun(
  qx: QueryExecutor,
  data: IDocReadinessRunStart,
): Promise<IDbDocReadinessRun> {
  return qx.selectOne(
    `
    INSERT INTO "projectDocReadinessRuns" (
      "trigger",
      "scope",
      "workflowId",
      "temporalRunId",
      "startedAt",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(trigger),
      $(scope),
      $(workflowId),
      $(temporalRunId),
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT ("temporalRunId") WHERE "temporalRunId" IS NOT NULL
    DO UPDATE SET "updatedAt" = NOW()
    RETURNING ${RUN_COLUMNS}
    `,
    {
      trigger: data.trigger,
      scope: data.scope,
      workflowId: data.workflowId ?? null,
      temporalRunId: data.temporalRunId ?? null,
    },
  )
}

// Guarded by "finishedAt" IS NULL so a run can only be finalized once.
export async function finishDocReadinessRun(
  qx: QueryExecutor,
  id: string,
  data: IDocReadinessRunFinish,
): Promise<IDbDocReadinessRun | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectDocReadinessRuns"
    SET
      "status" = $(status),
      "totalProjects" = $(totalProjects),
      "discovered" = $(discovered),
      "scored" = $(scored),
      "failed" = $(failed),
      "errorMessage" = $(errorMessage),
      "finishedAt" = NOW(),
      "updatedAt" = NOW()
    WHERE "id" = $(id) AND "finishedAt" IS NULL
    RETURNING ${RUN_COLUMNS}
    `,
    {
      id,
      status: data.status,
      totalProjects: data.totalProjects ?? null,
      discovered: data.discovered ?? null,
      scored: data.scored ?? null,
      failed: data.failed ?? null,
      errorMessage: data.errorMessage ?? null,
    },
  )
}

export async function findDocReadinessRunById(
  qx: QueryExecutor,
  id: string,
): Promise<IDbDocReadinessRun | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${RUN_COLUMNS}
    FROM "projectDocReadinessRuns"
    WHERE "id" = $(id)
    `,
    { id },
  )
}

export async function findLatestDocReadinessRun(
  qx: QueryExecutor,
  trigger?: DocReadinessRunTrigger,
): Promise<IDbDocReadinessRun | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${RUN_COLUMNS}
    FROM "projectDocReadinessRuns"
    WHERE ($(trigger)::text IS NULL OR "trigger" = $(trigger))
    ORDER BY "startedAt" DESC, "id" DESC
    LIMIT 1
    `,
    { trigger: trigger ?? null },
  )
}
