import { QueryExecutor } from '../queryExecutor'
import { prepareSelectColumns } from '../utils'

import {
  EvaluationStatus,
  IDbEvaluatedProject,
  IDbEvaluatedProjectCreate,
  IDbEvaluatedProjectUpdate,
} from './types'

const EVALUATED_PROJECT_COLUMNS = [
  'id',
  'projectCatalogId',
  'evaluationStatus',
  'evaluationScore',
  'evaluation',
  'evaluationReason',
  'evaluatedAt',
  'starsCount',
  'forksCount',
  'commitsCount',
  'pullRequestsCount',
  'issuesCount',
  'onboarded',
  'onboardedAt',
  'createdAt',
  'updatedAt',
]

export async function findEvaluatedProjectById(
  qx: QueryExecutor,
  id: string,
): Promise<IDbEvaluatedProject | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    FROM "evaluatedProjects"
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function findEvaluatedProjectByProjectCatalogId(
  qx: QueryExecutor,
  projectCatalogId: string,
): Promise<IDbEvaluatedProject | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    FROM "evaluatedProjects"
    WHERE "projectCatalogId" = $(projectCatalogId)
    `,
    { projectCatalogId },
  )
}

export async function findEvaluatedProjectsByStatus(
  qx: QueryExecutor,
  evaluationStatus: EvaluationStatus,
  options: { limit?: number; offset?: number } = {},
): Promise<IDbEvaluatedProject[]> {
  const { limit, offset } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    FROM "evaluatedProjects"
    WHERE "evaluationStatus" = $(evaluationStatus)
    ORDER BY "createdAt" ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { evaluationStatus, limit, offset },
  )
}

export async function findAllEvaluatedProjects(
  qx: QueryExecutor,
  options: { limit?: number; offset?: number } = {},
): Promise<IDbEvaluatedProject[]> {
  const { limit, offset } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    FROM "evaluatedProjects"
    ORDER BY "createdAt" DESC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { limit, offset },
  )
}

export async function countEvaluatedProjects(
  qx: QueryExecutor,
  evaluationStatus?: EvaluationStatus,
): Promise<number> {
  const statusFilter = evaluationStatus ? 'WHERE "evaluationStatus" = $(evaluationStatus)' : ''

  const result = await qx.selectOne(
    `
    SELECT COUNT(*) AS count
    FROM "evaluatedProjects"
    ${statusFilter}
    `,
    { evaluationStatus },
  )
  return parseInt(result.count, 10)
}

export async function insertEvaluatedProject(
  qx: QueryExecutor,
  data: IDbEvaluatedProjectCreate,
): Promise<IDbEvaluatedProject> {
  return qx.selectOne(
    `
    INSERT INTO "evaluatedProjects" (
      "projectCatalogId",
      "evaluationStatus",
      "evaluationScore",
      evaluation,
      "evaluationReason",
      "starsCount",
      "forksCount",
      "commitsCount",
      "pullRequestsCount",
      "issuesCount",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(projectCatalogId),
      $(evaluationStatus),
      $(evaluationScore),
      $(evaluation),
      $(evaluationReason),
      $(starsCount),
      $(forksCount),
      $(commitsCount),
      $(pullRequestsCount),
      $(issuesCount),
      NOW(),
      NOW()
    )
    RETURNING ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    `,
    {
      projectCatalogId: data.projectCatalogId,
      evaluationStatus: data.evaluationStatus ?? 'pending',
      evaluationScore: data.evaluationScore ?? null,
      evaluation: data.evaluation ? JSON.stringify(data.evaluation) : null,
      evaluationReason: data.evaluationReason ?? null,
      starsCount: data.starsCount ?? null,
      forksCount: data.forksCount ?? null,
      commitsCount: data.commitsCount ?? null,
      pullRequestsCount: data.pullRequestsCount ?? null,
      issuesCount: data.issuesCount ?? null,
    },
  )
}

export async function bulkInsertEvaluatedProjects(
  qx: QueryExecutor,
  items: IDbEvaluatedProjectCreate[],
): Promise<void> {
  if (items.length === 0) {
    return
  }

  const values = items.map((item) => ({
    projectCatalogId: item.projectCatalogId,
    evaluationStatus: item.evaluationStatus ?? 'pending',
    evaluationScore: item.evaluationScore ?? null,
    evaluation: item.evaluation ? JSON.stringify(item.evaluation) : null,
    evaluationReason: item.evaluationReason ?? null,
    starsCount: item.starsCount ?? null,
    forksCount: item.forksCount ?? null,
    commitsCount: item.commitsCount ?? null,
    pullRequestsCount: item.pullRequestsCount ?? null,
    issuesCount: item.issuesCount ?? null,
  }))

  await qx.result(
    `
    INSERT INTO "evaluatedProjects" (
      "projectCatalogId",
      "evaluationStatus",
      "evaluationScore",
      evaluation,
      "evaluationReason",
      "starsCount",
      "forksCount",
      "commitsCount",
      "pullRequestsCount",
      "issuesCount",
      "createdAt",
      "updatedAt"
    )
    SELECT
      v."projectCatalogId"::uuid,
      v."evaluationStatus",
      v."evaluationScore"::double precision,
      v.evaluation::jsonb,
      v."evaluationReason",
      v."starsCount"::integer,
      v."forksCount"::integer,
      v."commitsCount"::integer,
      v."pullRequestsCount"::integer,
      v."issuesCount"::integer,
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "projectCatalogId" text,
      "evaluationStatus" text,
      "evaluationScore" double precision,
      evaluation jsonb,
      "evaluationReason" text,
      "starsCount" integer,
      "forksCount" integer,
      "commitsCount" integer,
      "pullRequestsCount" integer,
      "issuesCount" integer
    )
    `,
    { values: JSON.stringify(values) },
  )
}

export async function updateEvaluatedProject(
  qx: QueryExecutor,
  id: string,
  data: IDbEvaluatedProjectUpdate,
): Promise<IDbEvaluatedProject | null> {
  const setClauses: string[] = []
  const params: Record<string, unknown> = { id }

  if (data.evaluationStatus !== undefined) {
    setClauses.push('"evaluationStatus" = $(evaluationStatus)')
    params.evaluationStatus = data.evaluationStatus
  }
  if (data.evaluationScore !== undefined) {
    setClauses.push('"evaluationScore" = $(evaluationScore)')
    params.evaluationScore = data.evaluationScore
  }
  if (data.evaluation !== undefined) {
    setClauses.push('evaluation = $(evaluation)')
    params.evaluation = data.evaluation ? JSON.stringify(data.evaluation) : null
  }
  if (data.evaluationReason !== undefined) {
    setClauses.push('"evaluationReason" = $(evaluationReason)')
    params.evaluationReason = data.evaluationReason
  }
  if (data.evaluatedAt !== undefined) {
    setClauses.push('"evaluatedAt" = $(evaluatedAt)')
    params.evaluatedAt = data.evaluatedAt
  }
  if (data.starsCount !== undefined) {
    setClauses.push('"starsCount" = $(starsCount)')
    params.starsCount = data.starsCount
  }
  if (data.forksCount !== undefined) {
    setClauses.push('"forksCount" = $(forksCount)')
    params.forksCount = data.forksCount
  }
  if (data.commitsCount !== undefined) {
    setClauses.push('"commitsCount" = $(commitsCount)')
    params.commitsCount = data.commitsCount
  }
  if (data.pullRequestsCount !== undefined) {
    setClauses.push('"pullRequestsCount" = $(pullRequestsCount)')
    params.pullRequestsCount = data.pullRequestsCount
  }
  if (data.issuesCount !== undefined) {
    setClauses.push('"issuesCount" = $(issuesCount)')
    params.issuesCount = data.issuesCount
  }
  if (data.onboarded !== undefined) {
    setClauses.push('onboarded = $(onboarded)')
    params.onboarded = data.onboarded
  }
  if (data.onboardedAt !== undefined) {
    setClauses.push('"onboardedAt" = $(onboardedAt)')
    params.onboardedAt = data.onboardedAt
  }

  if (setClauses.length === 0) {
    return findEvaluatedProjectById(qx, id)
  }

  return qx.selectOneOrNone(
    `
    UPDATE "evaluatedProjects"
    SET
      ${setClauses.join(',\n      ')},
      "updatedAt" = NOW()
    WHERE id = $(id)
    RETURNING ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    `,
    params,
  )
}

export async function markEvaluatedProjectAsEvaluated(
  qx: QueryExecutor,
  id: string,
  data: {
    evaluationScore: number
    evaluation: Record<string, unknown>
    evaluationReason?: string
  },
): Promise<IDbEvaluatedProject | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "evaluatedProjects"
    SET
      "evaluationStatus" = 'evaluated',
      "evaluationScore" = $(evaluationScore),
      evaluation = $(evaluation),
      "evaluationReason" = $(evaluationReason),
      "evaluatedAt" = NOW(),
      "updatedAt" = NOW()
    WHERE id = $(id)
    RETURNING ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS)}
    `,
    {
      id,
      evaluationScore: data.evaluationScore,
      evaluation: JSON.stringify(data.evaluation),
      evaluationReason: data.evaluationReason ?? null,
    },
  )
}

export async function markEvaluatedProjectAsOnboarded(
  qx: QueryExecutor,
  id: string,
): Promise<void> {
  await qx.selectNone(
    `
    UPDATE "evaluatedProjects"
    SET
      onboarded = true,
      "onboardedAt" = NOW(),
      "updatedAt" = NOW()
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function deleteEvaluatedProject(qx: QueryExecutor, id: string): Promise<number> {
  return qx.result(
    `
    DELETE FROM "evaluatedProjects"
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function deleteEvaluatedProjectByProjectCatalogId(
  qx: QueryExecutor,
  projectCatalogId: string,
): Promise<number> {
  return qx.result(
    `
    DELETE FROM "evaluatedProjects"
    WHERE "projectCatalogId" = $(projectCatalogId)
    `,
    { projectCatalogId },
  )
}

export async function findPendingEvaluatedProjectsWithCatalog(
  qx: QueryExecutor,
  options: { limit?: number } = {},
): Promise<(IDbEvaluatedProject & { projectSlug: string; repoName: string; repoUrl: string })[]> {
  const { limit } = options

  return qx.select(
    `
    SELECT
      ${prepareSelectColumns(EVALUATED_PROJECT_COLUMNS, 'ep')},
      pc."projectSlug",
      pc."repoName",
      pc."repoUrl"
    FROM "evaluatedProjects" ep
    JOIN "projectCatalog" pc ON pc.id = ep."projectCatalogId"
    WHERE ep."evaluationStatus" = 'pending'
    ORDER BY ep."createdAt" ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    `,
    { limit },
  )
}
