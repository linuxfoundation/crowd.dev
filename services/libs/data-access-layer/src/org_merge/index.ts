import { QueryExecutor } from '../queryExecutor'

export async function findOrgNoMergeIds(
  qx: QueryExecutor,
  organizationId: string,
): Promise<string[]> {
  const rows = await qx.select(
    `
      SELECT
        "noMergeId"
      FROM "organizationNoMerge"
      WHERE "organizationId" = $(organizationId)
    `,
    {
      organizationId,
    },
  )

  return rows.map((row: { noMergeId: string }) => row.noMergeId)
}

export async function removeOrganizationToMerge(
  qx: QueryExecutor,
  organizationId: string,
  toMergeId: string,
): Promise<void> {
  await qx.result(
    `
      WITH deleted_filtered AS (
        DELETE FROM "organizationToMerge"
        WHERE
          ("organizationId" = $(organizationId) AND "toMergeId" = $(toMergeId))
          OR
          ("organizationId" = $(toMergeId) AND "toMergeId" = $(organizationId))
      )
      DELETE FROM "organizationToMergeRaw"
      WHERE
        ("organizationId" = $(organizationId) AND "toMergeId" = $(toMergeId))
        OR
        ("organizationId" = $(toMergeId) AND "toMergeId" = $(organizationId))
    `,
    { organizationId, toMergeId },
  )
}

export async function removeOrganizationMergeSuggestions(
  qx: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qx.result(
    `
      WITH deleted_filtered AS (
        DELETE FROM "organizationToMerge"
        WHERE "organizationId" = $(organizationId)
           OR "toMergeId" = $(organizationId)
      )
      DELETE FROM "organizationToMergeRaw"
      WHERE "organizationId" = $(organizationId)
         OR "toMergeId" = $(organizationId)
    `,
    { organizationId },
  )
}

export async function insertOrganizationNoMerge(
  qx: QueryExecutor,
  organizationId: string,
  noMergeId: string,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO "organizationNoMerge" ("organizationId", "noMergeId", "createdAt", "updatedAt")
      SELECT $(organizationId), $(noMergeId), NOW(), NOW()
      WHERE EXISTS (SELECT 1 FROM organizations WHERE id = $(organizationId))
        AND EXISTS (SELECT 1 FROM organizations WHERE id = $(noMergeId))
      ON CONFLICT ("organizationId", "noMergeId") DO NOTHING
    `,
    { organizationId, noMergeId },
  )
}
