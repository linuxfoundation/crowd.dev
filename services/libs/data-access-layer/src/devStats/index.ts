import { getServiceChildLogger } from '@crowd/logging'
import { MemberIdentityType, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

const log = getServiceChildLogger('dev-stats:affiliations')

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
    (r) => !r.title || !BLACKLISTED_TITLES.some((t) => r.title?.toLowerCase().includes(t)),
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
      new Date(best.dateEnd ?? '9999-12-31').getTime() - new Date(best.dateStart ?? '').getTime()
    const currMs =
      new Date(curr.dateEnd ?? '9999-12-31').getTime() - new Date(curr.dateStart ?? '').getTime()
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

/** Returns the org used to fill gaps — primary undated wins, then earliest-created undated. */
function findFallbackOrg(rows: IDevStatsWorkRow[]): IDevStatsWorkRow | null {
  const primaryUndated = rows.find((r) => r.isPrimaryWorkExperience && !r.dateStart && !r.dateEnd)
  if (primaryUndated) return primaryUndated

  return (
    rows
      .filter((r) => !r.dateStart && !r.dateEnd)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .at(0) ?? null
  )
}

/**
 * Collects all date boundaries from the dated rows, capped at today.
 * Each dateStart and (dateEnd + 1 day) marks a point where active orgs can change.
 */
function collectBoundaries(datedRows: IDevStatsWorkRow[]): Date[] {
  const today = startOfDay(new Date())

  const ms = new Set<number>([today.getTime()])

  for (const row of datedRows) {
    const start = startOfDay(row.dateStart ?? '')
    if (start <= today) ms.add(start.getTime())

    if (row.dateEnd) {
      const afterEnd = startOfDay(row.dateEnd)
      afterEnd.setDate(afterEnd.getDate() + 1)
      if (afterEnd <= today) ms.add(afterEnd.getTime())
    }
  }

  return Array.from(ms)
    .sort((a, b) => a - b)
    .map((t) => new Date(t))
}

function orgsActiveAt(datedRows: IDevStatsWorkRow[], point: Date): IDevStatsWorkRow[] {
  return datedRows.filter((r) => {
    const start = startOfDay(r.dateStart ?? '')
    const end = r.dateEnd ? startOfDay(r.dateEnd) : null
    return point >= start && (!end || point <= end)
  })
}

function startOfDay(date: Date | string): Date {
  const d = new Date(date)
  d.setHours(0, 0, 0, 0)
  return d
}

function dayBefore(date: Date): Date {
  const d = new Date(date)
  d.setDate(d.getDate() - 1)
  return d
}

/** Iterates boundary intervals and builds non-overlapping affiliation segments. */
function buildTimeline(
  memberId: string,
  datedRows: IDevStatsWorkRow[],
  fallbackOrg: IDevStatsWorkRow | null,
  boundaries: Date[],
): IDevStatsAffiliation[] {
  const resolved: IDevStatsAffiliation[] = []
  let currentOrg: IDevStatsWorkRow | null = null
  let currentStart: Date | null = null
  let gapStart: Date | null = null

  const closeSegment = (org: IDevStatsWorkRow, start: Date, end: Date) => {
    log.debug(
      { memberId, org: org.organizationName, start: start.toISOString(), end: end.toISOString() },
      'closing segment',
    )
    resolved.push({
      organization: org.organizationName,
      startDate: start.toISOString(),
      endDate: end.toISOString(),
    })
  }

  for (let i = 0; i < boundaries.length - 1; i++) {
    const point = boundaries[i]
    const active = orgsActiveAt(datedRows, point)

    log.debug(
      { memberId, point: point.toISOString(), activeOrgs: active.map((r) => r.organizationName) },
      'processing boundary',
    )

    if (active.length === 0) {
      if (currentOrg && currentStart) {
        closeSegment(currentOrg, currentStart, dayBefore(point))
        currentOrg = null
        currentStart = null
      }
      if (gapStart === null) {
        gapStart = point
        log.debug({ memberId, gapStart: point.toISOString() }, 'gap started')
      }
      continue
    }

    if (gapStart !== null) {
      log.debug(
        {
          memberId,
          fallback: fallbackOrg?.organizationName ?? null,
          gapStart: gapStart.toISOString(),
          gapEnd: dayBefore(point).toISOString(),
        },
        'closing gap with fallback org',
      )
      if (fallbackOrg) closeSegment(fallbackOrg, gapStart, dayBefore(point))
      gapStart = null
    }

    const winner = selectPrimaryWorkExperience(active)

    if (!currentOrg) {
      log.debug(
        { memberId, org: winner.organizationName, from: point.toISOString() },
        'opening segment',
      )
      currentOrg = winner
      currentStart = point
    } else if (currentOrg.organizationId !== winner.organizationId) {
      log.debug(
        {
          memberId,
          from: currentOrg.organizationName,
          to: winner.organizationName,
          at: point.toISOString(),
        },
        'org changed',
      )
      closeSegment(currentOrg, currentStart ?? point, dayBefore(point))
      currentOrg = winner
      currentStart = point
    }
  }

  // Close the final open segment using the org's actual endDate (null = ongoing)
  if (currentOrg && currentStart) {
    const endDate = currentOrg.dateEnd ? new Date(currentOrg.dateEnd).toISOString() : null
    log.debug(
      { memberId, org: currentOrg.organizationName, start: currentStart.toISOString(), endDate },
      'closing final segment',
    )
    resolved.push({
      organization: currentOrg.organizationName,
      startDate: currentStart.toISOString(),
      endDate,
    })
  }

  // Close a trailing gap with the fallback org (ongoing, no endDate)
  if (gapStart !== null && fallbackOrg) {
    log.debug(
      { memberId, fallback: fallbackOrg.organizationName, gapStart: gapStart.toISOString() },
      'closing trailing gap with fallback org',
    )
    resolved.push({
      organization: fallbackOrg.organizationName,
      startDate: gapStart.toISOString(),
      endDate: null,
    })
  }

  return resolved
}

function resolveAffiliationsForMember(
  memberId: string,
  rows: IDevStatsWorkRow[],
): IDevStatsAffiliation[] {
  log.debug({ memberId, totalRows: rows.length }, 'resolving affiliations')

  // If one undated org is marked primary, drop all other undated orgs to avoid infinite conflicts
  const primaryUndated = rows.find((r) => r.isPrimaryWorkExperience && !r.dateStart && !r.dateEnd)
  const cleaned = primaryUndated
    ? rows.filter((r) => r.dateStart || r.id === primaryUndated.id)
    : rows

  if (cleaned.length < rows.length) {
    log.debug(
      { memberId, dropped: rows.length - cleaned.length },
      'dropped undated orgs (primary undated exists)',
    )
  }

  const fallbackOrg = findFallbackOrg(cleaned)
  const datedRows = cleaned.filter((r) => r.dateStart)

  log.debug(
    { memberId, datedRows: datedRows.length, fallbackOrg: fallbackOrg?.organizationName ?? null },
    'prepared rows',
  )

  if (datedRows.length === 0) {
    log.debug({ memberId }, 'no dated rows — returning empty affiliations')
    return []
  }

  const boundaries = collectBoundaries(datedRows)
  log.debug({ memberId, boundaries: boundaries.length }, 'collected boundaries')

  const timeline = buildTimeline(memberId, datedRows, fallbackOrg, boundaries)

  log.debug({ memberId, affiliations: timeline.length }, 'timeline built')

  return timeline.sort((a, b) => {
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
    result.set(id, resolveAffiliationsForMember(id, byMember.get(id) ?? []))
  }
  return result
}
