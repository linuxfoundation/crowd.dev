import { QueryExecutor } from '../queryExecutor'

export async function getOrphanMemberSegmentsAgg(
  qx: QueryExecutor,
  batchSize: number,
): Promise<string[]> {
  const rows = await qx.select(
    `
    SELECT msa."memberId"
    FROM "memberSegmentsAgg" msa
    LEFT JOIN members m ON msa."memberId" = m.id
    WHERE m.id IS NULL
    LIMIT $(batchSize)
    `,
    { batchSize },
  )

  return rows.map((r) => r.memberId)
}

export async function deleteOrphanMemberSegmentsAgg(
  qx: QueryExecutor,
  memberId: string,
): Promise<void> {
  await qx.result(
    `
    DELETE FROM "memberSegmentsAgg"
    WHERE "memberId" = $(memberId)
    `,
    { memberId },
  )
}

export async function deleteZeroActivityMemberSegmentsAgg(
  qx: QueryExecutor,
  batchSize: number,
): Promise<number> {
  return qx.result(
    `
    DELETE FROM "memberSegmentsAgg"
    WHERE ctid = ANY (
      ARRAY(
        SELECT ctid
        FROM "memberSegmentsAgg"
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
