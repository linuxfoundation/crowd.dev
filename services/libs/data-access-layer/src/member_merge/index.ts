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

  await qx.tx(async (tx) => {
    await tx.result(
      `
        DELETE FROM "memberToMerge"
        ${whereClause}
      `,
      replacements,
    )

    await tx.result(
      `
        DELETE FROM "memberToMergeRaw"
        ${whereClause}
      `,
      replacements,
    )
  })
}

export async function insertMemberNoMerge(
  qx: QueryExecutor,
  memberId: string,
  noMergeId: string,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO "memberNoMerge" ("memberId", "noMergeId", "createdAt", "updatedAt")
      SELECT $(memberId), $(noMergeId), NOW(), NOW()
      WHERE EXISTS (SELECT 1 FROM members WHERE id = $(memberId))
        AND EXISTS (SELECT 1 FROM members WHERE id = $(noMergeId))
      ON CONFLICT ("memberId", "noMergeId") DO NOTHING
    `,
    { memberId, noMergeId },
  )
}

export async function moveMemberNoMerge(
  qx: QueryExecutor,
  fromMemberId: string,
  toMemberId: string,
): Promise<void> {
  await qx.result(
    `
      with "blockedMembers" as (
        select distinct
          case when "memberId" = $(fromMemberId) then "noMergeId" else "memberId" end as id
        from "memberNoMerge"
        where "memberId" = $(fromMemberId) or "noMergeId" = $(fromMemberId)
      )
      insert into "memberNoMerge" ("memberId", "noMergeId", "createdAt", "updatedAt")
      select "memberId", "noMergeId", NOW(), NOW()
      from (
        select $(toMemberId)::uuid as "memberId", b.id as "noMergeId" from "blockedMembers" b
        union
        select b.id as "memberId", $(toMemberId)::uuid as "noMergeId" from "blockedMembers" b
      ) edges
      where "memberId" != "noMergeId"
      ON CONFLICT ("memberId", "noMergeId") DO NOTHING
    `,
    { fromMemberId, toMemberId },
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
