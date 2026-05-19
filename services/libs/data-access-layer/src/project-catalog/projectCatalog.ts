import { QueryExecutor } from '../queryExecutor'
import { prepareSelectColumns } from '../utils'

import {
  IDbProjectCatalog,
  IDbProjectCatalogCreate,
  IDbProjectCatalogUpdate,
  ProjectCatalogAction,
} from './types'

const PROJECT_CATALOG_COLUMNS = [
  'id',
  'projectSlug',
  'repoName',
  'repoUrl',
  'source',
  'action',
  'lfCriticalityScore',
  'evaluatedAt',
  'onboardedAt',
  'syncedAt',
  'createdAt',
  'updatedAt',
]

export async function findProjectCatalogById(
  qx: QueryExecutor,
  id: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function findProjectCatalogByRepoUrl(
  qx: QueryExecutor,
  repoUrl: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE "repoUrl" = $(repoUrl)
    `,
    { repoUrl },
  )
}

export async function findProjectCatalogBySlug(
  qx: QueryExecutor,
  projectSlug: string,
): Promise<IDbProjectCatalog[]> {
  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE "projectSlug" = $(projectSlug)
    ORDER BY "createdAt" DESC
    `,
    { projectSlug },
  )
}

export async function findAllProjectCatalog(
  qx: QueryExecutor,
  options: { limit?: number; offset?: number } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, offset } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    ORDER BY "createdAt" DESC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { limit, offset },
  )
}

export async function findProjectCatalogPendingEvaluation(
  qx: QueryExecutor,
  options: { limit?: number; offset?: number } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, offset } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE action = 'evaluate'
    ORDER BY "lfCriticalityScore" DESC NULLS LAST, "createdAt" ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { limit, offset },
  )
}

export async function countProjectCatalog(qx: QueryExecutor): Promise<number> {
  const result = await qx.selectOne(
    `
    SELECT COUNT(*) AS count
    FROM "projectCatalog"
    `,
  )
  return parseInt(result.count, 10)
}

export async function countProjectCatalogByAction(
  qx: QueryExecutor,
  action: ProjectCatalogAction,
): Promise<number> {
  const result = await qx.selectOne(
    `
    SELECT COUNT(*) AS count
    FROM "projectCatalog"
    WHERE action = $(action)
    `,
    { action },
  )
  return parseInt(result.count, 10)
}

/**
 * Promotes 'auto' projects to 'evaluate', enforcing a hard cap on the total
 * queue size in a single atomic statement.
 *
 * Uses a CTE to:
 *   1. Compute available slots (evaluateLimit − current 'evaluate' count) inline,
 *      so two concurrent calls observe the same baseline and each picks a
 *      disjoint set of rows.
 *   2. Lock candidates with FOR UPDATE SKIP LOCKED — concurrent transactions
 *      skip already-locked rows, preventing double-promotion.
 *   3. Re-check action = 'auto' in the outer UPDATE to guard against rows whose
 *      state changed after the subquery snapshot (e.g. manual updates).
 *
 * Ordering (configurable via `sourcePriority`):
 *   1. Source priority — earlier position in the array = higher priority (unlisted = lowest)
 *   2. lfCriticalityScore DESC (NULLs last)
 *   3. createdAt ASC (stable tie-breaker)
 *
 * Returns the number of rows actually promoted.
 */
export async function promoteProjectsToEvaluate(
  qx: QueryExecutor,
  options: { evaluateLimit: number; sourcePriority: string[] },
): Promise<number> {
  const { evaluateLimit, sourcePriority } = options

  return qx.result(
    `
    WITH
      slots AS (
        SELECT GREATEST(0, $(evaluateLimit) - COUNT(*)) AS available
        FROM "projectCatalog"
        WHERE action = 'evaluate'
      ),
      candidates AS (
        SELECT pc.id
        FROM "projectCatalog" pc
        CROSS JOIN slots
        WHERE pc.action = 'auto'
          AND slots.available > 0
        ORDER BY
          COALESCE(ARRAY_POSITION($(sourcePriority)::text[], pc.source), 2147483647) ASC,
          pc."lfCriticalityScore" DESC NULLS LAST,
          pc."createdAt" ASC
        LIMIT (SELECT available FROM slots)
        FOR UPDATE SKIP LOCKED
      )
    UPDATE "projectCatalog"
    SET action = 'evaluate', "updatedAt" = NOW()
    FROM candidates
    WHERE "projectCatalog".id = candidates.id
      AND "projectCatalog".action = 'auto'
    `,
    { evaluateLimit, sourcePriority },
  )
}

export async function insertProjectCatalog(
  qx: QueryExecutor,
  data: IDbProjectCatalogCreate,
): Promise<IDbProjectCatalog> {
  return qx.selectOne(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "action",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      $(source),
      $(action),
      $(lfCriticalityScore),
      NOW(),
      NOW(),
      NOW()
    )
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      projectSlug: data.projectSlug,
      repoName: data.repoName,
      repoUrl: data.repoUrl,
      source: data.source ?? null,
      action: data.action ?? 'auto',
      lfCriticalityScore: data.lfCriticalityScore ?? null,
    },
  )
}

export async function bulkInsertProjectCatalog(
  qx: QueryExecutor,
  items: IDbProjectCatalogCreate[],
): Promise<void> {
  if (items.length === 0) {
    return
  }

  const values = items.map((item) => ({
    projectSlug: item.projectSlug,
    repoName: item.repoName,
    repoUrl: item.repoUrl,
    source: item.source ?? null,
    action: item.action ?? 'auto',
    lfCriticalityScore: item.lfCriticalityScore ?? null,
  }))

  await qx.result(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "action",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    SELECT
      v."projectSlug",
      v."repoName",
      v."repoUrl",
      v."source",
      v."action",
      v."lfCriticalityScore"::double precision,
      NOW(),
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "projectSlug" text,
      "repoName" text,
      "repoUrl" text,
      "source" text,
      "action" text,
      "lfCriticalityScore" double precision
    )
    ON CONFLICT ("repoUrl") DO NOTHING
    `,
    { values: JSON.stringify(values) },
  )
}

export async function upsertProjectCatalog(
  qx: QueryExecutor,
  data: IDbProjectCatalogCreate,
): Promise<IDbProjectCatalog> {
  return qx.selectOne(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "action",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      $(source),
      $(action),
      $(lfCriticalityScore),
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "source" = COALESCE(EXCLUDED."source", "projectCatalog"."source"),
      "action" = CASE
        WHEN "projectCatalog"."action" IN ('onboard', 'unsure') THEN "projectCatalog"."action"
        WHEN EXCLUDED.action = 'evaluate' THEN 'evaluate'
        ELSE "projectCatalog"."action"
      END,
      "lfCriticalityScore" = COALESCE(EXCLUDED."lfCriticalityScore", "projectCatalog"."lfCriticalityScore"),
      "updatedAt" = NOW(),
      "syncedAt" = NOW()
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      projectSlug: data.projectSlug,
      repoName: data.repoName,
      repoUrl: data.repoUrl,
      source: data.source ?? null,
      action: data.action ?? 'auto',
      lfCriticalityScore: data.lfCriticalityScore ?? null,
    },
  )
}

export async function bulkUpsertProjectCatalog(
  qx: QueryExecutor,
  items: IDbProjectCatalogCreate[],
): Promise<void> {
  if (items.length === 0) {
    return
  }

  const values = items.map((item) => ({
    projectSlug: item.projectSlug,
    repoName: item.repoName,
    repoUrl: item.repoUrl,
    source: item.source ?? null,
    action: item.action ?? 'auto',
    lfCriticalityScore: item.lfCriticalityScore ?? null,
  }))

  await qx.result(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "action",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    SELECT
      v."projectSlug",
      v."repoName",
      v."repoUrl",
      v."source",
      v."action",
      v."lfCriticalityScore"::double precision,
      NOW(),
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "projectSlug" text,
      "repoName" text,
      "repoUrl" text,
      "source" text,
      "action" text,
      "lfCriticalityScore" double precision
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "source" = COALESCE(EXCLUDED."source", "projectCatalog"."source"),
      "action" = CASE
        WHEN "projectCatalog"."action" IN ('onboard', 'unsure') THEN "projectCatalog"."action"
        WHEN EXCLUDED.action = 'evaluate' THEN 'evaluate'
        ELSE "projectCatalog"."action"
      END,
      "lfCriticalityScore" = COALESCE(EXCLUDED."lfCriticalityScore", "projectCatalog"."lfCriticalityScore"),
      "updatedAt" = NOW(),
      "syncedAt" = NOW()
    `,
    { values: JSON.stringify(values) },
  )
}

export async function updateProjectCatalog(
  qx: QueryExecutor,
  id: string,
  data: IDbProjectCatalogUpdate,
): Promise<IDbProjectCatalog | null> {
  const setClauses: string[] = []
  const params: Record<string, unknown> = { id }

  if (data.projectSlug !== undefined) {
    setClauses.push('"projectSlug" = $(projectSlug)')
    params.projectSlug = data.projectSlug
  }
  if (data.repoName !== undefined) {
    setClauses.push('"repoName" = $(repoName)')
    params.repoName = data.repoName
  }
  if (data.repoUrl !== undefined) {
    setClauses.push('"repoUrl" = $(repoUrl)')
    params.repoUrl = data.repoUrl
  }
  if (data.source !== undefined) {
    setClauses.push('"source" = $(source)')
    params.source = data.source
  }
  if (data.action !== undefined) {
    setClauses.push('"action" = $(action)')
    params.action = data.action
  }
  if (data.lfCriticalityScore !== undefined) {
    setClauses.push('"lfCriticalityScore" = $(lfCriticalityScore)')
    params.lfCriticalityScore = data.lfCriticalityScore
  }
  if (data.syncedAt !== undefined) {
    setClauses.push('"syncedAt" = $(syncedAt)')
    params.syncedAt = data.syncedAt
  }
  if (data.evaluatedAt !== undefined) {
    setClauses.push('"evaluatedAt" = $(evaluatedAt)')
    params.evaluatedAt = data.evaluatedAt
  }
  if (data.onboardedAt !== undefined) {
    setClauses.push('"onboardedAt" = $(onboardedAt)')
    params.onboardedAt = data.onboardedAt
  }

  if (setClauses.length === 0) {
    return findProjectCatalogById(qx, id)
  }

  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET
      ${setClauses.join(',\n      ')},
      "updatedAt" = NOW()
    WHERE id = $(id)
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    params,
  )
}

export async function updateProjectCatalogSyncedAt(qx: QueryExecutor, id: string): Promise<void> {
  await qx.selectNone(
    `
    UPDATE "projectCatalog"
    SET "syncedAt" = NOW(), "updatedAt" = NOW()
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function deleteProjectCatalog(qx: QueryExecutor, id: string): Promise<number> {
  return qx.result(
    `
    DELETE FROM "projectCatalog"
    WHERE id = $(id)
    `,
    { id },
  )
}
