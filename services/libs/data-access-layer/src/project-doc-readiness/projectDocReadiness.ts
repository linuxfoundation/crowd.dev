import { QueryExecutor } from '../queryExecutor'
import {
  IDbProjectDocReadiness,
  IDbProjectDocReadinessCheck,
  IFindProjectsForDocsReadiness,
  IProjectDocReadinessCheckInsert,
  IProjectDocReadinessUpsert,
  IProjectForDocsReadiness,
} from './types'

const READINESS_COLUMNS = [
  'id',
  'projectId',
  'projectSlug',
  'projectName',
  'docsUrl',
  'discoveryMethod',
  'confidence',
  'isOverride',
  'overallScore',
  'overallGrade',
  'categoryScores',
  'runId',
  'durationMs',
  'ok',
  'error',
  'createdAt',
  'updatedAt',
]
  .map((c) => `"${c}"`)
  .concat(`"runDate"::text AS "runDate"`)
  .join(',\n')

const CHECK_COLUMNS = [
  'projectId',
  'checkId',
  'category',
  'status',
  'message',
  'details',
  'durationMs',
  'scoredAt',
  'createdAt',
  'updatedAt',
]
  .map((c) => `"${c}"`)
  .join(',\n')

// One row per project per run date; a same-day re-run overwrites the earlier result.
export async function upsertProjectDocReadiness(
  qx: QueryExecutor,
  data: IProjectDocReadinessUpsert,
): Promise<IDbProjectDocReadiness> {
  return qx.selectOne(
    `
    INSERT INTO "projectDocReadiness" (
      "projectId",
      "projectSlug",
      "projectName",
      "docsUrl",
      "discoveryMethod",
      "confidence",
      "isOverride",
      "overallScore",
      "overallGrade",
      "categoryScores",
      "runDate",
      "runId",
      "durationMs",
      "ok",
      "error",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(projectId),
      $(projectSlug),
      $(projectName),
      $(docsUrl),
      $(discoveryMethod),
      $(confidence),
      $(isOverride),
      $(overallScore),
      $(overallGrade),
      $(categoryScores)::jsonb,
      COALESCE($(runDate)::date, CURRENT_DATE),
      $(runId),
      $(durationMs),
      $(ok),
      $(error),
      NOW(),
      NOW()
    )
    ON CONFLICT ("projectId", "runDate") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "projectName" = EXCLUDED."projectName",
      "docsUrl" = EXCLUDED."docsUrl",
      "discoveryMethod" = EXCLUDED."discoveryMethod",
      "confidence" = EXCLUDED."confidence",
      "isOverride" = EXCLUDED."isOverride",
      "overallScore" = EXCLUDED."overallScore",
      "overallGrade" = EXCLUDED."overallGrade",
      "categoryScores" = EXCLUDED."categoryScores",
      "runId" = EXCLUDED."runId",
      "durationMs" = EXCLUDED."durationMs",
      "ok" = EXCLUDED."ok",
      "error" = EXCLUDED."error",
      "updatedAt" = NOW()
    RETURNING ${READINESS_COLUMNS}
    `,
    {
      ...data,
      categoryScores: data.categoryScores ? JSON.stringify(data.categoryScores) : null,
      runDate: data.runDate ?? null,
    },
  )
}

export async function findLatestProjectDocReadiness(
  qx: QueryExecutor,
  projectId: string,
): Promise<IDbProjectDocReadiness | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${READINESS_COLUMNS}
    FROM "projectDocReadiness"
    WHERE "projectId" = $(projectId)
    ORDER BY "runDate" DESC
    LIMIT 1
    `,
    { projectId },
  )
}

// Callers wrap this together with upsertProjectDocReadiness in one transaction.
export async function replaceProjectDocReadinessChecks(
  qx: QueryExecutor,
  projectId: string,
  checks: IProjectDocReadinessCheckInsert[],
): Promise<void> {
  await qx.tx(async (tx) => {
    await tx.selectNone(
      `DELETE FROM "projectDocReadinessChecks" WHERE "projectId" = $(projectId)`,
      {
        projectId,
      },
    )

    if (checks.length === 0) {
      return
    }

    await tx.selectNone(
      `
      INSERT INTO "projectDocReadinessChecks" (
        "projectId",
        "checkId",
        "category",
        "status",
        "message",
        "details",
        "durationMs",
        "scoredAt",
        "createdAt",
        "updatedAt"
      )
      SELECT
        $(projectId),
        c."checkId",
        c."category",
        c."status",
        c."message",
        c."details",
        c."durationMs",
        NOW(),
        NOW(),
        NOW()
      FROM jsonb_to_recordset($(checks)::jsonb) AS c(
        "checkId" TEXT,
        "category" TEXT,
        "status" TEXT,
        "message" TEXT,
        "details" TEXT,
        "durationMs" INTEGER
      )
      `,
      { projectId, checks: JSON.stringify(checks) },
    )
  })
}

export async function findProjectDocReadinessChecks(
  qx: QueryExecutor,
  projectId: string,
): Promise<IDbProjectDocReadinessCheck[]> {
  return qx.select(
    `
    SELECT ${CHECK_COLUMNS}
    FROM "projectDocReadinessChecks"
    WHERE "projectId" = $(projectId)
    ORDER BY "category", "checkId"
    `,
    { projectId },
  )
}

export async function findProjectsForDocsReadiness(
  qx: QueryExecutor,
  { mode, scope, afterId, limit }: IFindProjectsForDocsReadiness,
): Promise<IProjectForDocsReadiness[]> {
  return qx.select(
    `
    SELECT p."id", p."slug", p."name"
    FROM "insightsProjects" p
    LEFT JOIN LATERAL (
      SELECT r."ok"
      FROM "projectDocReadiness" r
      WHERE r."projectId" = p."id"
      ORDER BY r."runDate" DESC
      LIMIT 1
    ) latest ON TRUE
    WHERE p."enabled"
      AND p."deletedAt" IS NULL
      AND ($(lfOnly) = FALSE OR p."isLF")
      AND ($(incremental) = FALSE OR latest."ok" IS DISTINCT FROM TRUE)
      AND ($(afterId)::uuid IS NULL OR p."id" > $(afterId))
    ORDER BY p."id"
    LIMIT $(limit)
    `,
    {
      lfOnly: scope === 'lf',
      incremental: mode === 'incremental',
      afterId: afterId ?? null,
      limit,
    },
  )
}
