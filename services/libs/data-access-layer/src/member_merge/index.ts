import { QueryExecutor } from '../queryExecutor'

export async function removeMemberToMerge(
  qx: QueryExecutor,
  memberId: string,
  toMergeId: string,
): Promise<void> {
  const replacements = { memberId, toMergeId }

  const whereClause = `
    WHERE
      ("memberId" = $(memberId) AND "toMergeId" = $(toMergeId))
      OR
      ("memberId" = $(toMergeId) AND "toMergeId" = $(memberId))
  `

  for (const table of ['memberToMerge', 'memberToMergeRaw']) {
    await qx.result(
      `
        DELETE FROM "${table}"
        ${whereClause}
      `,
      replacements,
    )
  }
}

export async function insertMemberNoMerge(
  qx: QueryExecutor,
  memberId: string,
  noMergeId: string,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO "memberNoMerge" ("memberId", "noMergeId", "createdAt", "updatedAt")
      VALUES ($(memberId), $(noMergeId), NOW(), NOW())
      ON CONFLICT ("memberId", "noMergeId") DO NOTHING
    `,
    { memberId, noMergeId },
  )
}

export async function getMemberNoMerge(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; noMergeId: string }[]> {
  const rows = await qx.select(
    `select "memberId", "noMergeId" from "memberNoMerge" where "memberId" in ($(memberIds:csv)) or "noMergeId" in ($(memberIds:csv))`,
    { memberIds },
  )

  return rows
}
