import { MemberIdentityType, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

// ─── Public interfaces ────────────────────────────────────────────────────────

export interface IDevStatsMemberRow {
  githubHandle: string
  memberId: string
  displayName: string | null
}

// ─── Step 1: member lookup by GitHub handle ───────────────────────────────────

export async function findMembersByGithubHandles(
  qx: QueryExecutor,
  lowercasedHandles: string[],
): Promise<IDevStatsMemberRow[]> {
  return qx.select(
    `
      SELECT
        mi.value       AS "githubHandle",
        mi."memberId",
        m."displayName"
      FROM "memberIdentities" mi
      JOIN members m ON m.id = mi."memberId"
      WHERE mi.platform = $(platform)
        AND mi.type     = $(type)
        AND mi.verified = true
        AND lower(mi.value) IN ($(lowercasedHandles:csv))
        AND mi."deletedAt" IS NULL
        AND m."deletedAt"  IS NULL
    `,
    {
      platform: PlatformType.GITHUB,
      type: MemberIdentityType.USERNAME,
      lowercasedHandles,
    },
  )
}

// ─── Step 2: verified emails ──────────────────────────────────────────────────

export async function findVerifiedEmailsByMemberIds(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; email: string }[]> {
  return qx.select(
    `
      SELECT "memberId", value AS email
      FROM "memberIdentities"
      WHERE "memberId" IN ($(memberIds:csv))
        AND type       = $(type)
        AND verified   = true
        AND "deletedAt" IS NULL
    `,
    {
      memberIds,
      type: MemberIdentityType.EMAIL,
    },
  )
}
