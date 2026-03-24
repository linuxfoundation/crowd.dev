import { QueryExecutor } from '../queryExecutor'

export async function getOrphanOrganizationSegmentsAgg(
  qx: QueryExecutor,
  batchSize: number,
): Promise<string[]> {
  const rows = await qx.select(
    `
    SELECT osa."organizationId"
    FROM "organizationSegmentsAgg" osa
    LEFT JOIN organizations o ON osa."organizationId" = o.id
    WHERE o.id IS NULL
    LIMIT $(batchSize)
    `,
    { batchSize },
  )

  return rows.map((r) => r.organizationId)
}

export async function deleteOrphanOrganizationSegmentsAgg(
  qx: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qx.result(
    `
    DELETE FROM "organizationSegmentsAgg"
    WHERE "organizationId" = $(organizationId)
    `,
    { organizationId },
  )
}
