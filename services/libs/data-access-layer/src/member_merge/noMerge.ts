import { createHash } from 'crypto'

import { MemberIdentityType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

type MemberNoMergeRow = {
  memberId: string
  noMergeId: string
  evidenceHash: string | null
}

type VerifiedKeyRow = {
  memberId: string
  platform: string
  type: string
  value: string
}

async function verifiedKeysByMember(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Map<string, VerifiedKeyRow[]>> {
  if (memberIds.length === 0) return new Map()

  const rows: VerifiedKeyRow[] = await qx.select(
    `
      select "memberId", platform, type, lower(value) as value
      from "memberIdentities"
      where "memberId" in ($(memberIds:csv))
        and verified = true
        and "deletedAt" is null
    `,
    { memberIds },
  )

  const map = new Map<string, VerifiedKeyRow[]>()
  for (const id of memberIds) map.set(id, [])
  for (const row of rows) {
    const list = map.get(row.memberId)
    if (list) list.push(row)
  }
  return map
}

function hashFrom(
  byMember: Map<string, VerifiedKeyRow[]>,
  memberId: string,
  noMergeId: string,
): string {
  const [first, second] = [memberId, noMergeId].sort()
  const keys = [first, second].flatMap((id) =>
    (byMember.get(id) ?? []).map((r) => `${id}:${r.platform}:${r.type}:${r.value}`).sort(),
  )
  return createHash('sha256').update(keys.join('|')).digest('hex')
}

function shareVerifiedEmail(
  byMember: Map<string, VerifiedKeyRow[]>,
  memberId: string,
  noMergeId: string,
): boolean {
  const emails = new Set(
    (byMember.get(memberId) ?? [])
      .filter((r) => r.type === MemberIdentityType.EMAIL)
      .map((r) => r.value),
  )

  return (byMember.get(noMergeId) ?? []).some(
    (r) => r.type === MemberIdentityType.EMAIL && emails.has(r.value),
  )
}

export async function getMemberNoMerge(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; noMergeId: string }[]> {
  if (memberIds.length === 0) {
    return []
  }

  const rows: MemberNoMergeRow[] = await qx.select(
    `
      select "memberId", "noMergeId", "evidenceHash"
      from "memberNoMerge"
      where "memberId" in ($(memberIds:csv))
         or "noMergeId" in ($(memberIds:csv))
    `,
    { memberIds },
  )

  if (rows.length === 0) {
    return []
  }

  const involved = [...new Set(rows.flatMap((row) => [row.memberId, row.noMergeId]))]
  const byMember = await verifiedKeysByMember(qx, involved)

  return rows
    .filter((row) => {
      const current = hashFrom(byMember, row.memberId, row.noMergeId)
      if (!row.evidenceHash) {
        return !shareVerifiedEmail(byMember, row.memberId, row.noMergeId)
      }
      return row.evidenceHash === current
    })
    .map(({ memberId, noMergeId }) => ({ memberId, noMergeId }))
}

export async function insertMemberNoMerge(
  qx: QueryExecutor,
  memberId: string,
  noMergeId: string,
): Promise<void> {
  const byMember = await verifiedKeysByMember(qx, [memberId, noMergeId])
  const evidenceHash = hashFrom(byMember, memberId, noMergeId)

  await qx.result(
    `
      INSERT INTO "memberNoMerge" (
        "memberId",
        "noMergeId",
        "evidenceHash",
        "createdAt",
        "updatedAt"
      )
      VALUES
        ($(memberId), $(noMergeId), $(evidenceHash), NOW(), NOW()),
        ($(noMergeId), $(memberId), $(evidenceHash), NOW(), NOW())
      ON CONFLICT ("memberId", "noMergeId")
      DO UPDATE SET
        "evidenceHash" = EXCLUDED."evidenceHash",
        "updatedAt" = NOW()
    `,
    { memberId, noMergeId, evidenceHash },
  )
}

export async function removeMemberNoMerge(
  qx: QueryExecutor,
  pairs: { memberId: string; noMergeId: string }[],
): Promise<void> {
  if (pairs.length === 0) {
    return
  }

  await qx.result(
    `
      DELETE FROM "memberNoMerge" mnm
      USING (
        SELECT
          unnest($(memberIds)::uuid[]) AS "memberId",
          unnest($(noMergeIds)::uuid[]) AS "noMergeId"
      ) p
      WHERE
        (mnm."memberId" = p."memberId" AND mnm."noMergeId" = p."noMergeId")
        OR (mnm."memberId" = p."noMergeId" AND mnm."noMergeId" = p."memberId")
    `,
    {
      memberIds: pairs.map((pair) => pair.memberId),
      noMergeIds: pairs.map((pair) => pair.noMergeId),
    },
  )
}
