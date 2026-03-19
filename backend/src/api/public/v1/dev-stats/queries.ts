import { getServiceChildLogger } from '@crowd/logging'
import { MemberIdentityType, PlatformType } from '@crowd/types'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

const log = getServiceChildLogger('dev-stats-queries')

const BLACKLISTED_TITLES = ['investor', 'mentor', 'board member']

export interface AffiliationResult {
  githubHandle: string
  name: string | null
  emails: string[]
  affiliations: {
    organization: string
    startDate: string | null
    endDate: string | null
  }[]
}

export interface DevStatsQueryResult {
  contributors: AffiliationResult[]
  notFound: string[]
}

interface MemberRow {
  githubHandle: string
  memberId: string
  displayName: string | null
}

interface WorkExperienceRow {
  memberId: string
  organizationName: string
  title: string | null
  dateStart: string | null
  dateEnd: string | null
}

export async function findAffiliationsByGithubHandles(
  qx: QueryExecutor,
  githubHandles: string[],
): Promise<DevStatsQueryResult> {
  const t0 = performance.now()

  const lowercasedHandles = githubHandles.map((h) => h.toLowerCase())

  // Step 1: find verified members by github handles
  log.info(
    {
      query: `
        SELECT mi.value AS "githubHandle", mi."memberId", m."displayName"
        FROM "memberIdentities" mi
        JOIN members m ON m.id = mi."memberId"
        WHERE mi.platform = '${PlatformType.GITHUB}'
          AND mi.type = '${MemberIdentityType.USERNAME}'
          AND mi.verified = true
          AND lower(mi.value) IN (${lowercasedHandles.map((h) => `'${h}'`).join(', ')})
          AND mi."deletedAt" IS NULL
          AND m."deletedAt" IS NULL
      `,
    },
    'Step 1 query',
  )

  const memberRows: MemberRow[] = await qx.select(
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

  const t1 = performance.now()
  log.info(
    { handles: githubHandles.length, found: memberRows.length, ms: Math.round(t1 - t0) },
    'Step 1: members lookup',
  )

  const foundHandles = new Set(memberRows.map((r) => r.githubHandle.toLowerCase()))
  const notFound = githubHandles.filter((h) => !foundHandles.has(h.toLowerCase()))

  if (memberRows.length === 0) {
    return { contributors: [], notFound }
  }

  const memberIds = memberRows.map((r) => r.memberId)

  // Step 2: fetch verified emails for found members
  log.info(
    {
      query: `
        SELECT "memberId", value AS email
        FROM "memberIdentities"
        WHERE "memberId" IN (${memberIds.map((id) => `'${id}'`).join(', ')})
          AND type = '${MemberIdentityType.EMAIL}'
          AND verified = true
          AND "deletedAt" IS NULL
      `,
    },
    'Step 2 query',
  )

  const emailRows: { memberId: string; email: string }[] = await qx.select(
    `
      SELECT "memberId", value AS email
      FROM "memberIdentities"
      WHERE "memberId" IN ($(memberIds:csv))
        AND type       = $(emailType)
        AND verified   = true
        AND "deletedAt" IS NULL
    `,
    {
      memberIds,
      emailType: MemberIdentityType.EMAIL,
    },
  )

  const emailsByMember = new Map<string, string[]>()
  for (const row of emailRows) {
    const list = emailsByMember.get(row.memberId) ?? []
    list.push(row.email)
    emailsByMember.set(row.memberId, list)
  }

  const t2 = performance.now()
  log.info(
    { members: memberIds.length, emails: emailRows.length, ms: Math.round(t2 - t1) },
    'Step 2: emails lookup',
  )

  // Step 3: fetch work experiences — excluding blocked affiliations
  log.info(
    {
      query: `
        SELECT mo."memberId", o."displayName" AS "organizationName", mo.title, mo."dateStart", mo."dateEnd"
        FROM "memberOrganizations" mo
        JOIN organizations o ON mo."organizationId" = o.id
        LEFT JOIN "memberOrganizationAffiliationOverrides" ovr ON ovr."memberOrganizationId" = mo.id
        WHERE mo."memberId" IN (${memberIds.map((id) => `'${id}'`).join(', ')})
          AND mo."deletedAt" IS NULL
          AND COALESCE(ovr."allowAffiliation", true) = true
      `,
    },
    'Step 3 query',
  )

  const workExperienceRows: WorkExperienceRow[] = await qx.select(
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

  const t3 = performance.now()
  log.info(
    { members: memberIds.length, ms: Math.round(t3 - t2) },
    'Step 3: work experiences lookup',
  )

  // group work experiences by memberId, filtering blacklisted titles
  const workExpByMember = new Map<string, WorkExperienceRow[]>()
  for (const row of workExperienceRows) {
    if (row.title && BLACKLISTED_TITLES.some((t) => row.title.toLowerCase().includes(t))) {
      continue
    }
    const list = workExpByMember.get(row.memberId) ?? []
    list.push(row)
    workExpByMember.set(row.memberId, list)
  }

  // Step 4: build response
  const contributors: AffiliationResult[] = memberRows.map((member) => {
    const workExperiences = workExpByMember.get(member.memberId) ?? []

    const affiliations = workExperiences
      .sort((a, b) => {
        if (!a.dateStart) return 1
        if (!b.dateStart) return -1
        return new Date(b.dateStart).getTime() - new Date(a.dateStart).getTime()
      })
      .map((we) => ({
        organization: we.organizationName,
        startDate: we.dateStart ? new Date(we.dateStart).toISOString() : null,
        endDate: we.dateEnd ? new Date(we.dateEnd).toISOString() : null,
      }))

    return {
      githubHandle: member.githubHandle,
      name: member.displayName,
      emails: emailsByMember.get(member.memberId) ?? [],
      affiliations,
    }
  })

  log.info(
    {
      handles: githubHandles.length,
      found: contributors.length,
      notFound: notFound.length,
      totalMs: Math.round(t3 - t0),
    },
    'dev-stats affiliations query complete',
  )

  return { contributors, notFound }
}
