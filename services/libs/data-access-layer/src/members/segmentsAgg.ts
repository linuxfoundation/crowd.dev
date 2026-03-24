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
