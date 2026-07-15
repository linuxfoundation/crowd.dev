import { QueryExecutor } from '../queryExecutor'

export async function removeMemberToMerge(
  qx: QueryExecutor,
  memberId: string,
  toMergeId: string,
): Promise<void> {
  await qx.result(
    `
      DELETE FROM "memberToMerge"
      WHERE "memberId" = $(memberId)
        AND "toMergeId" = $(toMergeId)
    `,
    {
      memberId,
      toMergeId,
    },
  )
  await qx.result(
    `
      DELETE FROM "memberToMergeRaw"
      WHERE "memberId" = $(memberId)
        AND "toMergeId" = $(toMergeId)
    `,
    {
      memberId,
      toMergeId,
    },
  )
}

export async function addMemberNoMerge(
  qx: QueryExecutor,
  memberId: string,
  noMergeId: string,
): Promise<void> {
  const currentTime = new Date()
  await qx.result(
    `
      INSERT INTO "memberNoMerge" ("memberId", "noMergeId", "createdAt", "updatedAt")
      VALUES ($(memberId), $(noMergeId), $(createdAt), $(updatedAt))
      on conflict ("memberId", "noMergeId") do nothing
    `,
    {
      memberId,
      noMergeId,
      createdAt: currentTime,
      updatedAt: currentTime,
    },
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
