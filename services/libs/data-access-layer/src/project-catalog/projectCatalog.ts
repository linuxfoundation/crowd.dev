import { QueryExecutor } from '../queryExecutor'
import { prepareSelectColumns } from '../utils'

import { IDbProjectCatalog, IDbProjectCatalogCreate, IDbProjectCatalogUpdate } from './types'

const PROJECT_CATALOG_COLUMNS = [
  'id',
  'projectSlug',
  'repoName',
  'repoUrl',
  'ossfCriticalityScore',
  'lfCriticalityScore',
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

export async function countProjectCatalog(qx: QueryExecutor): Promise<number> {
  const result = await qx.selectOne(
    `
    SELECT COUNT(*) AS count
    FROM "projectCatalog"
    `,
  )
  return parseInt(result.count, 10)
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
      "ossfCriticalityScore",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      $(ossfCriticalityScore),
      $(lfCriticalityScore),
      NOW(),
      NOW()
    )
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      projectSlug: data.projectSlug,
      repoName: data.repoName,
      repoUrl: data.repoUrl,
      ossfCriticalityScore: data.ossfCriticalityScore ?? null,
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
    ossfCriticalityScore: item.ossfCriticalityScore ?? null,
    lfCriticalityScore: item.lfCriticalityScore ?? null,
  }))

  await qx.result(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "ossfCriticalityScore",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt"
    )
    SELECT
      v."projectSlug",
      v."repoName",
      v."repoUrl",
      v."ossfCriticalityScore"::double precision,
      v."lfCriticalityScore"::double precision,
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "projectSlug" text,
      "repoName" text,
      "repoUrl" text,
      "ossfCriticalityScore" double precision,
      "lfCriticalityScore" double precision
    )
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
      "ossfCriticalityScore",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      $(ossfCriticalityScore),
      $(lfCriticalityScore),
      NOW(),
      NOW()
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "ossfCriticalityScore" = COALESCE(EXCLUDED."ossfCriticalityScore", "projectCatalog"."ossfCriticalityScore"),
      "lfCriticalityScore" = COALESCE(EXCLUDED."lfCriticalityScore", "projectCatalog"."lfCriticalityScore"),
      "updatedAt" = NOW()
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      projectSlug: data.projectSlug,
      repoName: data.repoName,
      repoUrl: data.repoUrl,
      ossfCriticalityScore: data.ossfCriticalityScore ?? null,
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
    ossfCriticalityScore: item.ossfCriticalityScore ?? null,
    lfCriticalityScore: item.lfCriticalityScore ?? null,
  }))

  await qx.result(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "ossfCriticalityScore",
      "lfCriticalityScore",
      "createdAt",
      "updatedAt"
    )
    SELECT
      v."projectSlug",
      v."repoName",
      v."repoUrl",
      v."ossfCriticalityScore"::double precision,
      v."lfCriticalityScore"::double precision,
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "projectSlug" text,
      "repoName" text,
      "repoUrl" text,
      "ossfCriticalityScore" double precision,
      "lfCriticalityScore" double precision
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "ossfCriticalityScore" = COALESCE(EXCLUDED."ossfCriticalityScore", "projectCatalog"."ossfCriticalityScore"),
      "lfCriticalityScore" = COALESCE(EXCLUDED."lfCriticalityScore", "projectCatalog"."lfCriticalityScore"),
      "updatedAt" = NOW()
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
  if (data.ossfCriticalityScore !== undefined) {
    setClauses.push('"ossfCriticalityScore" = $(ossfCriticalityScore)')
    params.ossfCriticalityScore = data.ossfCriticalityScore
  }
  if (data.lfCriticalityScore !== undefined) {
    setClauses.push('"lfCriticalityScore" = $(lfCriticalityScore)')
    params.lfCriticalityScore = data.lfCriticalityScore
  }
  if (data.syncedAt !== undefined) {
    setClauses.push('"syncedAt" = $(syncedAt)')
    params.syncedAt = data.syncedAt
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
