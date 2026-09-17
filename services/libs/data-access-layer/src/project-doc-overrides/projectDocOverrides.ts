import { QueryExecutor } from '../queryExecutor'

import { IDbProjectDocOverride, IProjectDocOverrideCreate } from './types'

const OVERRIDE_COLUMNS = [
  'id',
  'projectId',
  'docsUrl',
  'submittedBy',
  'submittedAt',
  'active',
  'createdAt',
  'updatedAt',
]
  .map((c) => `"${c}"`)
  .join(',\n')

// Overrides keep their history: the previous active row is deactivated and a
// new active row is inserted, in one transaction so the partial unique index
// on (projectId) WHERE active never sees two active rows.
export async function createProjectDocOverride(
  qx: QueryExecutor,
  data: IProjectDocOverrideCreate,
): Promise<IDbProjectDocOverride> {
  return qx.tx(async (tx) => {
    await deactivateProjectDocOverride(tx, data.projectId)

    return tx.selectOne(
      `
      INSERT INTO "projectDocOverrides" (
        "projectId",
        "docsUrl",
        "submittedBy",
        "submittedAt",
        "active",
        "createdAt",
        "updatedAt"
      )
      VALUES (
        $(projectId),
        $(docsUrl),
        $(submittedBy),
        NOW(),
        TRUE,
        NOW(),
        NOW()
      )
      RETURNING ${OVERRIDE_COLUMNS}
      `,
      data,
    )
  })
}

export async function deactivateProjectDocOverride(
  qx: QueryExecutor,
  projectId: string,
): Promise<IDbProjectDocOverride | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectDocOverrides"
    SET "active" = FALSE, "updatedAt" = NOW()
    WHERE "projectId" = $(projectId) AND "active"
    RETURNING ${OVERRIDE_COLUMNS}
    `,
    { projectId },
  )
}

export async function findActiveProjectDocOverride(
  qx: QueryExecutor,
  projectId: string,
): Promise<IDbProjectDocOverride | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${OVERRIDE_COLUMNS}
    FROM "projectDocOverrides"
    WHERE "projectId" = $(projectId) AND "active"
    `,
    { projectId },
  )
}
