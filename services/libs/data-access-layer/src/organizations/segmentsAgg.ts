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

export async function deleteZeroActivityOrganizationSegmentsAgg(
  qx: QueryExecutor,
  batchSize: number,
): Promise<number> {
  return qx.result(
    `
    DELETE FROM "organizationSegmentsAgg"
    WHERE ctid = ANY (
      ARRAY(
        SELECT ctid
        FROM "organizationSegmentsAgg"
        WHERE "activityCount" = 0
        LIMIT $(batchSize)
      )
    )
    -- re-checked on rows a sink updated after the batch was selected
    AND "activityCount" = 0
    `,
    { batchSize },
  )
}
