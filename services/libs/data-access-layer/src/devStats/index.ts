import { MemberIdentityType, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

const BLACKLISTED_TITLES = ['investor', 'mentor', 'board member']

export interface IDevStatsMemberRow {
  githubHandle: string
  memberId: string
  displayName: string | null
}

export interface IDevStatsWorkExperience {
  memberId: string
  organizationName: string
  title: string | null
  dateStart: string | null
  dateEnd: string | null
}

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

export async function findWorkExperiencesByMemberIds(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IDevStatsWorkExperience[]> {
  const rows: IDevStatsWorkExperience[] = await qx.select(
    `
      SELECT
        mo."memberId",
        o."displayName" AS "organizationName",
        mo.title,
        mo."dateStart",
        mo."dateEnd"
      FROM "memberOrganizations" mo
      JOIN organizations o ON mo."organizationId" = o.id
      LEFT JOIN "memberOrganizationAffiliationOverrides" ovr ON ovr."memberOrganizationId" = mo.id
      WHERE mo."memberId" IN ($(memberIds:csv))
        AND mo."deletedAt" IS NULL
        AND COALESCE(ovr."allowAffiliation", true) = true
    `,
    { memberIds },
  )

  return rows.filter(
    (r) =>
      !r.title || !BLACKLISTED_TITLES.some((t) => r.title.toLowerCase().includes(t)),
  )
}
