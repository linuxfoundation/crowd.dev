import { getServiceChildLogger } from '@crowd/logging'

import { QueryExecutor } from '../queryExecutor'

const log = getServiceChildLogger('affiliations:resolve')

const BLACKLISTED_TITLES = ['investor', 'mentor', 'board member']

export interface IAffiliationPeriod {
  organization: string
  startDate: string | null
  endDate: string | null
}

interface IWorkRow {
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
  /** null for memberOrganizations rows; non-null for memberSegmentAffiliations rows */
  segmentId: string | null
}

export async function findWorkExperiencesBulk(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IWorkRow[]> {
  const rows: IWorkRow[] = await qx.select(
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
    (r) => !r.title || !BLACKLISTED_TITLES.some((t) => r.title?.toLowerCase().includes(t)),
  )
}

export async function findManualAffiliationsBulk(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<IWorkRow[]> {
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

// ─── Selection priority ───────────────────────────────────────────────────────

function durationMs(org: IWorkRow): number {
  const start = new Date(org.dateStart ?? '').getTime()
  const end = new Date(org.dateEnd ?? '9999-12-31').getTime()
  return end - start
}

function longestDateRange(orgs: IWorkRow[]): IWorkRow {
  const withDates = orgs.filter((r) => r.dateStart)
  const candidates = withDates.length > 0 ? withDates : orgs

  let best = candidates[0]

  for (const org of candidates) {
    if (durationMs(org) > durationMs(best)) best = org
  }

  return best
}

function selectPrimaryWorkExperience(orgs: IWorkRow[]): IWorkRow {
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

// ─── Timeline helpers ─────────────────────────────────────────────────────────

/** Returns the org used to fill gaps — primary undated wins, then earliest-created undated. */
function findFallbackOrg(rows: IWorkRow[]): IWorkRow | null {
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
function collectBoundaries(datedRows: IWorkRow[]): Date[] {
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

function orgsActiveAt(rows: IWorkRow[], boundaryDate: Date): IWorkRow[] {
  return rows.filter((role) => {
    if (!role.dateStart) return true // undated: active at every boundary

    const roleStart = startOfDay(role.dateStart)
    const roleEnd = role.dateEnd ? startOfDay(role.dateEnd) : null

    // org is active if the boundary date falls within its employment period
    return boundaryDate >= roleStart && (!roleEnd || boundaryDate <= roleEnd)
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

function closeAffiliationWindow(
  memberId: string,
  affiliations: IAffiliationPeriod[],
  org: IWorkRow,
  windowStart: Date,
  windowEnd: Date,
): void {
  log.debug(
    {
      memberId,
      org: org.organizationName,
      windowStart: windowStart.toISOString(),
      windowEnd: windowEnd.toISOString(),
    },
    'closing affiliation window',
  )
  affiliations.push({
    organization: org.organizationName,
    startDate: windowStart.toISOString(),
    endDate: windowEnd.toISOString(),
  })
}

/** Iterates boundary intervals and builds non-overlapping affiliation windows. */
function buildTimeline(
  memberId: string,
  allRows: IWorkRow[],
  fallbackOrg: IWorkRow | null,
  boundaries: Date[],
): IAffiliationPeriod[] {
  const affiliations: IAffiliationPeriod[] = []
  let currentOrg: IWorkRow = null
  let currentWindowStart: Date = null
  let uncoveredPeriodStart: Date = null

  for (let i = 0; i < boundaries.length; i++) {
    const boundaryDate = boundaries[i]
    const activeOrgsAtBoundary = orgsActiveAt(allRows, boundaryDate)

    log.debug(
      {
        memberId,
        boundaryDate: boundaryDate.toISOString(),
        orgsAtBoundary: activeOrgsAtBoundary.map((r) => ({
          org: r.organizationName,
          dateStart: r.dateStart,
          dateEnd: r.dateEnd,
          isPrimary: r.isPrimaryWorkExperience,
          memberCount: r.memberCount,
          isManual: r.segmentId !== null,
        })),
      },
      'processing boundary',
    )

    // No orgs active at this boundary — close the current window and start tracking a gap
    if (activeOrgsAtBoundary.length === 0) {
      if (currentOrg && currentWindowStart) {
        closeAffiliationWindow(
          memberId,
          affiliations,
          currentOrg,
          currentWindowStart,
          dayBefore(boundaryDate),
        )
        currentOrg = null
        currentWindowStart = null
      }

      if (uncoveredPeriodStart === null) {
        uncoveredPeriodStart = boundaryDate
        log.debug(
          { memberId, uncoveredPeriodStart: boundaryDate.toISOString() },
          'uncovered period started',
        )
      }

      continue
    }

    // Orgs are active again — close the uncovered period using the fallback org if available
    if (uncoveredPeriodStart !== null) {
      log.debug(
        {
          memberId,
          fallbackOrg: fallbackOrg?.organizationName ?? null,
          uncoveredPeriodStart: uncoveredPeriodStart.toISOString(),
          uncoveredPeriodEnd: dayBefore(boundaryDate).toISOString(),
        },
        'closing uncovered period with fallback org',
      )

      if (fallbackOrg) {
        closeAffiliationWindow(
          memberId,
          affiliations,
          fallbackOrg,
          uncoveredPeriodStart,
          dayBefore(boundaryDate),
        )
      }

      uncoveredPeriodStart = null
    }

    const winningAffiliation = selectPrimaryWorkExperience(activeOrgsAtBoundary)

    // No current window open — start a new one with the winning org
    if (!currentOrg) {
      log.debug(
        { memberId, org: winningAffiliation.organizationName, from: boundaryDate.toISOString() },
        'opening affiliation window',
      )
      currentOrg = winningAffiliation
      currentWindowStart = boundaryDate
      continue
    }

    // Winning org changed — close the current window and open a new one
    if (currentOrg.organizationId !== winningAffiliation.organizationId) {
      log.debug(
        {
          memberId,
          from: currentOrg.organizationName,
          to: winningAffiliation.organizationName,
          at: boundaryDate.toISOString(),
        },
        'affiliation changed',
      )
      closeAffiliationWindow(
        memberId,
        affiliations,
        currentOrg,
        currentWindowStart ?? boundaryDate,
        dayBefore(boundaryDate),
      )
      currentOrg = winningAffiliation
      currentWindowStart = boundaryDate
    }
  }

  // Close the last open window using the org's actual end date (null = ongoing)
  if (currentOrg && currentWindowStart) {
    const endDate = currentOrg.dateEnd ? new Date(currentOrg.dateEnd).toISOString() : null
    log.debug(
      {
        memberId,
        org: currentOrg.organizationName,
        start: currentWindowStart.toISOString(),
        endDate,
      },
      'closing final affiliation window',
    )
    affiliations.push({
      organization: currentOrg.organizationName,
      startDate: currentWindowStart.toISOString(),
      endDate,
    })
  }

  // Close a trailing uncovered period using the fallback org (ongoing, no end date)
  if (uncoveredPeriodStart !== null && fallbackOrg) {
    log.debug(
      {
        memberId,
        fallbackOrg: fallbackOrg.organizationName,
        uncoveredPeriodStart: uncoveredPeriodStart.toISOString(),
      },
      'closing trailing uncovered period with fallback org',
    )
    affiliations.push({
      organization: fallbackOrg.organizationName,
      startDate: uncoveredPeriodStart.toISOString(),
      endDate: null,
    })
  }

  return affiliations
}

// ─── Per-member resolution ────────────────────────────────────────────────────

function resolveAffiliationsForMember(memberId: string, rows: IWorkRow[]): IAffiliationPeriod[] {
  // If one undated work-experience org is marked primary, drop other undated work-experience orgs
  // to avoid infinite conflicts. Manual affiliations (segmentId !== null) are never dropped.
  const primaryUndated = rows.find((r) => r.isPrimaryWorkExperience && !r.dateStart && !r.dateEnd)
  const cleaned = primaryUndated
    ? rows.filter((r) => r.segmentId !== null || r.dateStart || r.id === primaryUndated.id)
    : rows

  const fallbackOrg = findFallbackOrg(cleaned)
  const datedRows = cleaned.filter((r) => r.dateStart)

  log.debug(
    {
      memberId,
      datedRows: datedRows.length,
      undatedRows: cleaned.length - datedRows.length,
      fallbackOrg: fallbackOrg?.organizationName ?? null,
      datedRowsList: datedRows.map((r) => ({
        org: r.organizationName,
        dateStart: r.dateStart,
        dateEnd: r.dateEnd,
      })),
    },
    'prepared rows',
  )

  if (datedRows.length === 0) {
    if (fallbackOrg) {
      log.debug(
        { memberId, fallbackOrg: fallbackOrg.organizationName },
        'no dated rows — returning fallback as undated affiliation',
      )
      return [{ organization: fallbackOrg.organizationName, startDate: null, endDate: null }]
    }
    log.debug({ memberId }, 'no dated rows and no fallback — returning empty affiliations')
    return []
  }

  const boundaries = collectBoundaries(datedRows)
  log.debug(
    {
      memberId,
      boundaries: boundaries.length,
      boundaryDates: boundaries.map((b) => b.toISOString()),
    },
    'collected boundaries',
  )

  // Pass all cleaned rows (not just dated) so undated orgs compete at every boundary (bug 2 fix)
  const timeline = buildTimeline(memberId, cleaned, fallbackOrg, boundaries)

  log.debug(
    {
      memberId,
      affiliations: timeline.length,
      result: timeline.map((a) => ({
        org: a.organization,
        startDate: a.startDate,
        endDate: a.endDate,
      })),
    },
    'timeline built',
  )

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
): Promise<Map<string, IAffiliationPeriod[]>> {
  const [workExperiences, manualAffiliations] = await Promise.all([
    findWorkExperiencesBulk(qx, memberIds),
    findManualAffiliationsBulk(qx, memberIds),
  ])

  const byMember = new Map<string, IWorkRow[]>()
  for (const row of [...workExperiences, ...manualAffiliations]) {
    const list = byMember.get(row.memberId) ?? []
    list.push(row)
    byMember.set(row.memberId, list)
  }

  const result = new Map<string, IAffiliationPeriod[]>()
  for (const id of memberIds) {
    result.set(id, resolveAffiliationsForMember(id, byMember.get(id) ?? []))
  }
  return result
}
