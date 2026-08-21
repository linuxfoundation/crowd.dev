import { QueryExecutor } from '../queryExecutor'

export async function removeMemberToMerge(
  qx: QueryExecutor,
  memberId: string,
  toMergeId: string,
): Promise<void> {
  await qx.result(
    `
      WITH deleted_filtered AS (
        DELETE FROM "memberToMerge"
        WHERE
          ("memberId" = $(memberId) AND "toMergeId" = $(toMergeId))
          OR
          ("memberId" = $(toMergeId) AND "toMergeId" = $(memberId))
      )
      DELETE FROM "memberToMergeRaw"
      WHERE
        ("memberId" = $(memberId) AND "toMergeId" = $(toMergeId))
        OR
        ("memberId" = $(toMergeId) AND "toMergeId" = $(memberId))
    `,
    { memberId, toMergeId },
  )
}

export async function removeMemberMergeSuggestions(
  qx: QueryExecutor,
  memberId: string,
): Promise<void> {
  await qx.result(
    `
      WITH deleted_filtered AS (
        DELETE FROM "memberToMerge"
        WHERE "memberId" = $(memberId)
           OR "toMergeId" = $(memberId)
      )
      DELETE FROM "memberToMergeRaw"
      WHERE "memberId" = $(memberId)
         OR "toMergeId" = $(memberId)
    `,
    { memberId },
  )
}

export async function insertMemberNoMerge(
  qx: QueryExecutor,
  memberId: string,
  noMergeId: string,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO "memberNoMerge" ("memberId", "noMergeId", "createdAt", "updatedAt")
      VALUES
        ($(memberId), $(noMergeId), NOW(), NOW()),
        ($(noMergeId), $(memberId), NOW(), NOW())
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
