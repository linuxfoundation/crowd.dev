import { getLongestDateRange } from '@crowd/common'
import { IMemberOrganization } from '@crowd/types'

import { BLACKLISTED_MEMBER_TITLES } from '../members/base'
import { QueryExecutor } from '../queryExecutor'

export interface IAffiliationPeriod {
  organization: string
  startDate: string | null
  endDate: string | null
}

interface IWorkExperienceResolution {
  id: string
  memberId: string
  organizationId: string
  organizationName: string
  title: string | null
  dateStart: string | null
  dateEnd: string | null
  createdAt: Date | string
  isPrimaryWorkExperience: boolean
  memberCount: number
  segmentId: string | null
}

/**
 * this intentionally differs from the equivalent query in member-organization-affiliation/index.ts
 * which uses organizationSegmentsAgg to compute memberCount. This is because the api should be faster
 */
export async function findWorkExperiencesBulk(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IWorkExperienceResolution[]> {
  const rows: IWorkExperienceResolution[] = await qx.select(
    `
      WITH relevant_orgs AS (
        SELECT DISTINCT "organizationId"
        FROM "memberOrganizations"
        WHERE "memberId" IN ($(memberIds:csv))
          AND "deletedAt" IS NULL
      ),
      aggs AS (
        SELECT "organizationId", COUNT(DISTINCT "memberId") AS total_count
        FROM "memberOrganizations"
        WHERE "organizationId" IN (SELECT "organizationId" FROM relevant_orgs)
          AND "deletedAt" IS NULL
        GROUP BY "organizationId"
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
    (r) => !r.title || !BLACKLISTED_MEMBER_TITLES.some((t) => r.title?.toLowerCase().includes(t)),
  )
}

export async function findManualAffiliationsBulk(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IWorkExperienceResolution[]> {
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
        NULL::timestamptz AS "createdAt",
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

function selectPrimaryWorkExperience(orgs: IWorkExperienceResolution[]) {
  if (orgs.length === 1) return orgs[0]

  // 1. Manual affiliations (segmentId non-null) always win
  const manual = orgs.filter((r) => r.segmentId !== null)
  if (manual.length > 0) {
    if (manual.length === 1) return manual[0]
    return getLongestDateRange(
      manual as unknown as IMemberOrganization[],
    ) as unknown as IWorkExperienceResolution
  }

  // 2. isPrimaryWorkExperience = true — prefer those with a dateStart
  const primary = orgs.filter((r) => r.isPrimaryWorkExperience)
  if (primary.length > 0) return primary.find((r) => r.dateStart) ?? primary[0]

  // 3. Only one org has a dateStart — pick it
  const withDates = orgs.filter((r) => r.dateStart)
  if (withDates.length === 1) return withDates[0]

  // 4. Org with strictly more members wins; if tied, fall through
  const sorted = [...orgs].sort((a, b) => b.memberCount - a.memberCount)
  if (sorted.length >= 2 && sorted[0].memberCount > sorted[1].memberCount) {
    return sorted[0]
  }

  // 5. Longest date range as final tiebreaker
  return getLongestDateRange(
    orgs as unknown as IMemberOrganization[],
  ) as unknown as IWorkExperienceResolution
}

/** Returns the org used to fill gaps — primary undated wins, then earliest-created undated. */
function findFallbackOrg(rows: IWorkExperienceResolution[]): IWorkExperienceResolution | null {
  const primaryUndated = rows.find((r) => r.isPrimaryWorkExperience && !r.dateStart && !r.dateEnd)
  if (primaryUndated) return primaryUndated

  return (
    rows
      .filter((r) => !r.dateStart && !r.dateEnd)
      .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())
      .at(0) ?? null
  )
}

/**
 * Collects all date boundaries from the dated rows, capped at today.
 * Each dateStart and (dateEnd + 1 day) marks a point where active orgs can change.
 */
function collectBoundaries(datedRows: IWorkExperienceResolution[]): Date[] {
  const today = startOfDay(new Date())

  const ms = new Set<number>([today.getTime()])

  for (const row of datedRows) {
    const start = startOfDay(row.dateStart ?? '')
    if (start <= today) ms.add(start.getTime())

    if (row.dateEnd) {
      const afterEnd = startOfDay(row.dateEnd)
      afterEnd.setUTCDate(afterEnd.getUTCDate() + 1)
      if (afterEnd <= today) ms.add(afterEnd.getTime())
    }
  }

  return Array.from(ms)
    .sort((a, b) => a - b)
    .map((t) => new Date(t))
}

function orgsActiveAt(
  rows: IWorkExperienceResolution[],
  boundaryDate: Date,
): IWorkExperienceResolution[] {
  return rows.filter((role) => {
    if (!role.dateStart && !role.dateEnd) return true // truly undated: active at every boundary

    const roleStart = role.dateStart ? startOfDay(role.dateStart) : null
    const roleEnd = role.dateEnd ? startOfDay(role.dateEnd) : null

    // org is active if the boundary date falls within its employment period
    return (!roleStart || boundaryDate >= roleStart) && (!roleEnd || boundaryDate <= roleEnd)
  })
}

function startOfDay(date: Date | string): Date {
  const d = new Date(date)
  d.setUTCHours(0, 0, 0, 0)
  return d
}

function dayBefore(date: Date): Date {
  const d = new Date(date)
  d.setUTCDate(d.getUTCDate() - 1)
  return d
}

/** Iterates boundary intervals and builds non-overlapping affiliation windows. */
function buildTimeline(
  allRows: IWorkExperienceResolution[],
  fallbackOrg: IWorkExperienceResolution | null,
  boundaries: Date[],
): IAffiliationPeriod[] {
  const affiliations: IAffiliationPeriod[] = []
  let currentOrg: IWorkExperienceResolution = null
  let currentWindowStart: Date = null
  let uncoveredPeriodStart: Date = null

  for (const boundaryDate of boundaries) {
    const activeOrgsAtBoundary = orgsActiveAt(allRows, boundaryDate)

    // No orgs active at this boundary — close the current window and start tracking a gap
    if (activeOrgsAtBoundary.length === 0) {
      if (currentOrg && currentWindowStart) {
        affiliations.push({
          organization: currentOrg.organizationName,
          startDate: currentWindowStart.toISOString(),
          endDate: dayBefore(boundaryDate).toISOString(),
        })
        currentOrg = null
        currentWindowStart = null
      }

      if (uncoveredPeriodStart === null) {
        uncoveredPeriodStart = boundaryDate
      }

      continue
    }

    // Orgs are active again — close the uncovered period using the fallback org if available
    if (uncoveredPeriodStart !== null) {
      if (fallbackOrg) {
        affiliations.push({
          organization: fallbackOrg.organizationName,
          startDate: uncoveredPeriodStart.toISOString(),
          endDate: dayBefore(boundaryDate).toISOString(),
        })
      }

      uncoveredPeriodStart = null
    }

    const winningAffiliation = selectPrimaryWorkExperience(activeOrgsAtBoundary)

    // No current window open — start a new one with the winning org
    if (!currentOrg) {
      currentOrg = winningAffiliation
      currentWindowStart = boundaryDate
      continue
    }

    // Winning org changed — close the current window and open a new one
    if (currentOrg.organizationId !== winningAffiliation.organizationId) {
      affiliations.push({
        organization: currentOrg.organizationName,
        startDate: currentWindowStart.toISOString(),
        endDate: dayBefore(boundaryDate).toISOString(),
      })
      currentOrg = winningAffiliation
      currentWindowStart = boundaryDate
    } else if (currentOrg.id !== winningAffiliation.id) {
      // Same org, different record — update so the final window closure uses the correct dateEnd
      currentOrg = winningAffiliation
    }
  }

  // Close the last open window using the org's actual end date (null = ongoing)
  if (currentOrg && currentWindowStart) {
    affiliations.push({
      organization: currentOrg.organizationName,
      startDate: currentWindowStart.toISOString(),
      endDate: currentOrg.dateEnd ? new Date(currentOrg.dateEnd).toISOString() : null,
    })
  }

  // Close a trailing uncovered period using the fallback org (ongoing, no end date)
  if (uncoveredPeriodStart !== null && fallbackOrg) {
    affiliations.push({
      organization: fallbackOrg.organizationName,
      startDate: uncoveredPeriodStart.toISOString(),
      endDate: null,
    })
  }

  return affiliations
}

function resolveAffiliationsForMember(rows: IWorkExperienceResolution[]): IAffiliationPeriod[] {
  // If one undated work-experience org is marked primary, drop other undated work-experience orgs
  // to avoid infinite conflicts. Manual affiliations (segmentId !== null) are never dropped.
  const primaryUndated = rows.find((r) => r.isPrimaryWorkExperience && !r.dateStart && !r.dateEnd)
  const cleaned = primaryUndated
    ? rows.filter((r) => r.segmentId !== null || r.dateStart || r.id === primaryUndated.id)
    : rows

  const fallbackOrg = findFallbackOrg(cleaned)
  const datedRows = cleaned.filter((r) => r.dateStart)

  if (datedRows.length === 0) {
    if (fallbackOrg) {
      return [{ organization: fallbackOrg.organizationName, startDate: null, endDate: null }]
    }
    return []
  }

  const boundaries = collectBoundaries(datedRows)

  // Pass all cleaned rows (not just dated) so undated orgs compete at every boundary (bug 2 fix)
  const timeline = buildTimeline(cleaned, fallbackOrg, boundaries)

  return timeline.sort((a, b) => {
    if (!a.startDate) return 1
    if (!b.startDate) return -1
    return new Date(b.startDate).getTime() - new Date(a.startDate).getTime()
  })
}

export async function resolveAffiliationsByMemberIds(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Map<string, IAffiliationPeriod[]>> {
  const [workExperiences, manualAffiliations] = await Promise.all([
    findWorkExperiencesBulk(qx, memberIds),
    findManualAffiliationsBulk(qx, memberIds),
  ])

  const byMember = new Map<string, IWorkExperienceResolution[]>()
  for (const row of [...workExperiences, ...manualAffiliations]) {
    const list = byMember.get(row.memberId) ?? []
    list.push(row)
    byMember.set(row.memberId, list)
  }

  const result = new Map<string, IAffiliationPeriod[]>()
  for (const id of memberIds) {
    result.set(id, resolveAffiliationsForMember(byMember.get(id) ?? []))
  }
  return result
}
