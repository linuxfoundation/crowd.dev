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
  const replacements = { organizationId, toMergeId }

  const whereClause = `
    WHERE
      ("organizationId" = $(organizationId) AND "toMergeId" = $(toMergeId))
      OR
      ("organizationId" = $(toMergeId) AND "toMergeId" = $(organizationId))
  `

  await qx.tx(async (tx) => {
    await tx.result(
      `
        DELETE FROM "organizationToMerge"
        ${whereClause}
      `,
      replacements,
    )

    await tx.result(
      `
        DELETE FROM "organizationToMergeRaw"
        ${whereClause}
      `,
      replacements,
    )
  })
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
