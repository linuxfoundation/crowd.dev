import { dateIntersects, getMemberOrganizationSourceRank } from '@crowd/common'
import { normalizeMemberOrganizationDate } from '@crowd/common_services'
import type { IMemberOrganization, IMemberRoleWithOrganization } from '@crowd/types'
import { OrganizationSource } from '@crowd/types'

function memberOrganizationsOverlap<T extends IMemberOrganization>(a: T, b: T): boolean {
  return (
    a.organizationId === b.organizationId &&
    dateIntersects(
      normalizeMemberOrganizationDate(a.dateStart),
      normalizeMemberOrganizationDate(a.dateEnd),
      normalizeMemberOrganizationDate(b.dateStart),
      normalizeMemberOrganizationDate(b.dateEnd),
    )
  )
}

/**
 * Finds email-domain rows that are represented by the same visible work experience.
 */
export function getOverlappingEmailDomainMemberOrganizations<T extends IMemberOrganization>(
  rows: T[],
  memberOrganization: T,
): T[] {
  return rows.filter(
    (row) =>
      row.id !== memberOrganization.id &&
      row.source === OrganizationSource.EMAIL_DOMAIN &&
      memberOrganizationsOverlap(row, memberOrganization),
  )
}

/**
 * Groups overlapping email-domain rows into the best non-email-domain display row.
 */
export function groupMemberOrganizations<T extends IMemberOrganization>(rows: T[]): T[] {
  const emailDomainRows = rows.filter((row) => row.source === OrganizationSource.EMAIL_DOMAIN)
  const nonEmailRows = rows.filter((row) => row.source !== OrganizationSource.EMAIL_DOMAIN)
  const hiddenEmailDomainIds = new Set<string>()
  const displayGroups = new Map<string, { displayRow: T; groupedEmailDomainRows: T[] }>()

  for (const emailDomainRow of emailDomainRows.filter(
    (row): row is T & { id: string } => !!row.id,
  )) {
    const overlappingNonEmailRows = nonEmailRows.filter((row) =>
      memberOrganizationsOverlap(emailDomainRow, row),
    )

    if (overlappingNonEmailRows.length > 0) {
      const displayRow = [...overlappingNonEmailRows].sort((a, b) => {
        const rankDiff =
          getMemberOrganizationSourceRank(a.source) - getMemberOrganizationSourceRank(b.source)
        if (rankDiff !== 0) {
          return rankDiff
        }

        return (a.id ?? '').localeCompare(b.id ?? '')
      })[0]

      if (displayRow.id) {
        hiddenEmailDomainIds.add(emailDomainRow.id)

        const existingGroup = displayGroups.get(displayRow.id)
        if (existingGroup) {
          existingGroup.groupedEmailDomainRows.push(emailDomainRow)
        } else {
          displayGroups.set(displayRow.id, {
            displayRow,
            groupedEmailDomainRows: [emailDomainRow],
          })
        }
      }
    }
  }

  return rows
    .filter((row): row is T & { id: string } => !!row.id && !hiddenEmailDomainIds.has(row.id))
    .map((row) => {
      const group = displayGroups.get(row.id)
      if (!group) {
        return row
      }

      // Preserve the visible row while surfacing the combined sources and date range
      const groupedRows = [group.displayRow, ...group.groupedEmailDomainRows]

      const normalizedStarts = groupedRows
        .map((groupedRow) => normalizeMemberOrganizationDate(groupedRow.dateStart))
        .filter((date): date is string => date !== null)

      const normalizedEnds = groupedRows.map((groupedRow) =>
        normalizeMemberOrganizationDate(groupedRow.dateEnd),
      )

      const sources = new Set<string>()

      for (const groupedRow of groupedRows) {
        if (groupedRow.source) {
          sources.add(groupedRow.source)
        }
      }

      let dateEnd: string | null = null

      if (normalizedEnds.some((date) => date === null)) {
        dateEnd = null
      } else if (normalizedEnds.length > 0) {
        const datedEnds = normalizedEnds.filter((date): date is string => date !== null)
        dateEnd = datedEnds.reduce((max, date) => (date > max ? date : max))
      }

      return {
        ...row,
        source: [...sources]
          .sort((a, b) => getMemberOrganizationSourceRank(a) - getMemberOrganizationSourceRank(b))
          .join(','),
        dateStart:
          normalizedStarts.length > 0
            ? normalizedStarts.reduce((min, date) => (date < min ? date : min))
            : null,
        dateEnd,
      }
    })
}

export function toMemberWorkExperience(mo: IMemberRoleWithOrganization, domains: string[] = []) {
  return {
    id: mo.id,
    organizationId: mo.organizationId,
    organizationName: mo.organizationName,
    organizationLogo: mo.organizationLogo,
    domains,
    jobTitle: mo.title ?? null,
    verified: mo.verified ?? false,
    verifiedBy: mo.verifiedBy ?? null,
    source: mo.source ?? null,
    startDate: mo.dateStart ?? null,
    endDate: mo.dateEnd ?? null,
    createdAt: mo.createdAt ?? null,
    updatedAt: mo.updatedAt ?? null,
  }
}
