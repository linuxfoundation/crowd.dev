import _ from 'lodash'

import { MemberIdentityType, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

// ─── Constants ────────────────────────────────────────────────────────────────

const BLACKLISTED_TITLES = ['investor', 'mentor', 'board member']

// ─── Public interfaces ────────────────────────────────────────────────────────

export interface IDevStatsMemberRow {
  githubHandle: string
  memberId: string
  displayName: string | null
}

export interface IDevStatsAffiliation {
  organization: string
  startDate: string | null
  endDate: string | null
}

// ─── Internal row type (union of memberOrganizations + manual affiliations) ───

interface IDevStatsWorkRow {
  id: string
  memberId: string
  organizationId: string
  organizationName: string
  title: string | null
  dateStart: string | null
  dateEnd: string | null
  createdAt: string
  isPrimaryWorkExperience: boolean
  memberCount: number
  /** null for memberOrganizations rows; non-null for memberSegmentAffiliations rows */
  segmentId: string | null
}

// ─── Step 1: member lookup ────────────────────────────────────────────────────

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

// ─── Step 3a: regular work experiences (bulk) ─────────────────────────────────

async function findWorkExperiencesBulk(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IDevStatsWorkRow[]> {
  const rows: IDevStatsWorkRow[] = await qx.select(
    `
      WITH aggs AS (
        SELECT
          osa."organizationId",
          sum(osa."memberCount") AS total_count
        FROM "organizationSegmentsAgg" osa
        WHERE osa."segmentId" IN (
          SELECT id FROM segments
          WHERE "grandparentId" IS NOT NULL
            AND "parentId"      IS NOT NULL
        )
        GROUP BY osa."organizationId"
      )
      SELECT
        mo.id,
        mo."memberId",
        mo."organizationId",
        o."displayName"                                     AS "organizationName",
        mo.title,
        mo."dateStart",
        mo."dateEnd",
        mo."createdAt",
        COALESCE(ovr."isPrimaryWorkExperience", false)      AS "isPrimaryWorkExperience",
        COALESCE(a.total_count, 0)                          AS "memberCount",
        NULL::text                                          AS "segmentId"
      FROM "memberOrganizations" mo
      JOIN organizations o ON mo."organizationId" = o.id
      LEFT JOIN "memberOrganizationAffiliationOverrides" ovr ON ovr."memberOrganizationId" = mo.id
      LEFT JOIN aggs a ON a."organizationId" = mo."organizationId"
      WHERE mo."memberId" IN ($(memberIds:csv))
        AND mo."deletedAt" IS NULL
        AND COALESCE(ovr."allowAffiliation", true) = true
    `,
    { memberIds },
  )

  return rows.filter(
    (r) => !r.title || !BLACKLISTED_TITLES.some((t) => r.title!.toLowerCase().includes(t)),
  )
}

// ─── Step 3b: manual affiliations (bulk) ─────────────────────────────────────

async function findManualAffiliationsBulk(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IDevStatsWorkRow[]> {
  return qx.select(
    `
      SELECT
        msa.id,
        msa."memberId",
        msa."organizationId",
        o."displayName"   AS "organizationName",
        NULL              AS title,
        msa."dateStart",
        msa."dateEnd",
        msa."createdAt",
        false             AS "isPrimaryWorkExperience",
        0                 AS "memberCount",
        msa."segmentId"
      FROM "memberSegmentAffiliations" msa
      JOIN organizations o ON msa."organizationId" = o.id
      WHERE msa."memberId" IN ($(memberIds:csv))
    `,
    { memberIds },
  )
}

// ─── Selection priority (mirrors selectPrimaryWorkExperience) ─────────────────

function longestDateRange(orgs: IDevStatsWorkRow[]): IDevStatsWorkRow {
  const withDates = orgs.filter((r) => r.dateStart)
  if (withDates.length === 0) return orgs[0]

  return withDates.reduce((best, curr) => {
    const bestMs =
      new Date(best.dateEnd ?? '9999-12-31').getTime() - new Date(best.dateStart!).getTime()
    const currMs =
      new Date(curr.dateEnd ?? '9999-12-31').getTime() - new Date(curr.dateStart!).getTime()
    return currMs > bestMs ? curr : best
  })
}

function selectPrimaryWorkExperience(orgs: IDevStatsWorkRow[]): IDevStatsWorkRow {
  if (orgs.length === 1) return orgs[0]

  // 1. Manual affiliations (segmentId non-null) always win
  const manual = orgs.filter((r) => r.segmentId !== null)
  if (manual.length > 0) {
    if (manual.length === 1) return manual[0]
    return longestDateRange(manual)
  }

  // 2. isPrimaryWorkExperience = true — prefer those with a dateStart
  const primary = orgs.filter((r) => r.isPrimaryWorkExperience)
  if (primary.length > 0) {
    const withDates = primary.filter((r) => r.dateStart)
    if (withDates.length > 0) return withDates[0]
    return primary[0]
  }

  // 3. Only one org has a dateStart — pick it
  const withDates = orgs.filter((r) => r.dateStart)
  if (withDates.length === 1) return withDates[0]

  // 4. Org with strictly more members wins; if tied, fall through
  const sorted = [...orgs].sort((a, b) => b.memberCount - a.memberCount)
  if (sorted.length >= 2 && sorted[0].memberCount > sorted[1].memberCount) {
    return sorted[0]
  }

  // 5. Longest date range as final tiebreaker
  return longestDateRange(orgs)
}

// ─── Per-member affiliation resolution ───────────────────────────────────────

function resolveAffiliationsForMember(rows: IDevStatsWorkRow[]): IDevStatsAffiliation[] {
  // Undated org cleanup: if one undated org is marked as primary, drop all other undated orgs
  const primaryUndated = rows.find(
    (r) => r.isPrimaryWorkExperience && !r.dateStart && !r.dateEnd,
  )
  const cleaned = primaryUndated
    ? rows.filter((r) => r.dateStart || r.id === primaryUndated.id)
    : rows

  // Fallback org: covers gaps and pre-history; primary undated takes precedence,
  // otherwise use the earliest-created undated org
  const fallbackOrg =
    primaryUndated ??
    (_.chain(cleaned)
      .filter((r) => !r.dateStart && !r.dateEnd)
      .sortBy('createdAt')
      .head()
      .value() as IDevStatsWorkRow | undefined) ??
    null

  const datedRows = cleaned.filter((r) => r.dateStart)
  if (datedRows.length === 0) {
    return []
  }

  // Collect date boundaries: each dateStart and (dateEnd + 1 day) as interval edges
  const today = new Date()
  today.setHours(0, 0, 0, 0)

  const boundarySet = new Set<number>()
  for (const row of datedRows) {
    const start = new Date(row.dateStart!)
    start.setHours(0, 0, 0, 0)
    if (start.getTime() <= today.getTime()) {
      boundarySet.add(start.getTime())
    }

    if (row.dateEnd) {
      const afterEnd = new Date(row.dateEnd)
      afterEnd.setHours(0, 0, 0, 0)
      afterEnd.setDate(afterEnd.getDate() + 1)
      if (afterEnd.getTime() <= today.getTime()) {
        boundarySet.add(afterEnd.getTime())
      }
    }
  }
  boundarySet.add(today.getTime())

  const boundaries = Array.from(boundarySet).sort((a, b) => a - b)

  const resolved: IDevStatsAffiliation[] = []
  let currentOrg: IDevStatsWorkRow | null = null
  let currentStart: Date | null = null
  let gapStart: Date | null = null

  for (let i = 0; i < boundaries.length - 1; i++) {
    const intervalStart = new Date(boundaries[i])

    // Orgs active at the start of this interval
    const active = datedRows.filter((r) => {
      const start = new Date(r.dateStart!)
      start.setHours(0, 0, 0, 0)
      const end = r.dateEnd ? new Date(r.dateEnd) : null
      if (end) end.setHours(0, 0, 0, 0)
      return intervalStart >= start && (!end || intervalStart <= end)
    })

    if (active.length === 0) {
      // Gap — close current org segment if open
      if (currentOrg && currentStart) {
        const dayBefore = new Date(intervalStart)
        dayBefore.setDate(dayBefore.getDate() - 1)
        resolved.push({
          organization: currentOrg.organizationName,
          startDate: currentStart.toISOString(),
          endDate: dayBefore.toISOString(),
        })
        currentOrg = null
        currentStart = null
      }
      if (gapStart === null) gapStart = new Date(intervalStart)
    } else {
      // Close gap with fallback org if present
      if (gapStart !== null) {
        const dayBefore = new Date(intervalStart)
        dayBefore.setDate(dayBefore.getDate() - 1)
        if (fallbackOrg) {
          resolved.push({
            organization: fallbackOrg.organizationName,
            startDate: gapStart.toISOString(),
            endDate: dayBefore.toISOString(),
          })
        }
        gapStart = null
      }

      const winner = selectPrimaryWorkExperience(active)

      if (!currentOrg) {
        currentOrg = winner
        currentStart = new Date(intervalStart)
      } else if (currentOrg.organizationId !== winner.organizationId) {
        // Org changed — close previous segment and open a new one
        const dayBefore = new Date(intervalStart)
        dayBefore.setDate(dayBefore.getDate() - 1)
        resolved.push({
          organization: currentOrg.organizationName,
          startDate: currentStart!.toISOString(),
          endDate: dayBefore.toISOString(),
        })
        currentOrg = winner
        currentStart = new Date(intervalStart)
      }
    }
  }

  // Close the final open segment
  if (currentOrg && currentStart) {
    resolved.push({
      organization: currentOrg.organizationName,
      startDate: currentStart.toISOString(),
      endDate: currentOrg.dateEnd ? new Date(currentOrg.dateEnd).toISOString() : null,
    })
  }

  // Close a trailing gap with the fallback org
  if (gapStart !== null && fallbackOrg) {
    resolved.push({
      organization: fallbackOrg.organizationName,
      startDate: gapStart.toISOString(),
      endDate: null,
    })
  }

  // Most recent affiliations first
  return resolved.sort((a, b) => {
    if (!a.startDate) return 1
    if (!b.startDate) return -1
    return new Date(b.startDate).getTime() - new Date(a.startDate).getTime()
  })
}

// ─── Public bulk resolver ─────────────────────────────────────────────────────

export async function resolveAffiliationsByMemberIds(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Map<string, IDevStatsAffiliation[]>> {
  const [workExperiences, manualAffiliations] = await Promise.all([
    findWorkExperiencesBulk(qx, memberIds),
    findManualAffiliationsBulk(qx, memberIds),
  ])

  const byMember = new Map<string, IDevStatsWorkRow[]>()
  for (const row of [...workExperiences, ...manualAffiliations]) {
    const list = byMember.get(row.memberId) ?? []
    list.push(row)
    byMember.set(row.memberId, list)
  }

  const result = new Map<string, IDevStatsAffiliation[]>()
  for (const id of memberIds) {
    result.set(id, resolveAffiliationsForMember(byMember.get(id) ?? []))
  }
  return result
}
