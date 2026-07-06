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

function isCollapsibleMemberOrganization<T extends IMemberOrganization>(row: T): boolean {
  if (!row.source) {
    return false
  }

  return row.source
    .split(',')
    .map((value) => value.trim())
    .some((value) =>
      [
        OrganizationSource.EMAIL_DOMAIN,
        OrganizationSource.PROJECT_REGISTRY,
      ].includes(value as OrganizationSource),
    )
}

function compareMemberOrganizationsBySourceRank(
  a: IMemberOrganization,
  b: IMemberOrganization,
): number {
  const rankDiff =
    getMemberOrganizationSourceRank(a.source) - getMemberOrganizationSourceRank(b.source)
  if (rankDiff !== 0) {
    return rankDiff
  }

  return (a.id ?? '').localeCompare(b.id ?? '')
}

/** Hidden inferential rows that overlap a visible work experience (for delete/update/override). */
export function getOverlappingGroupedMemberOrganizations<T extends IMemberOrganization>(
  rows: T[],
  memberOrganization: T,
): T[] {
  return rows.filter(
    (row) =>
      row.id !== memberOrganization.id &&
      isCollapsibleMemberOrganization(row) &&
      memberOrganizationsOverlap(row, memberOrganization),
  )
}

function canDisplayCollapsibleRow<T extends IMemberOrganization>(
  displayRow: T,
  collapsibleRow: T,
): boolean {
  if (!isCollapsibleMemberOrganization(displayRow)) {
    return true
  }

  return (
    getMemberOrganizationSourceRank(displayRow.source) <
    getMemberOrganizationSourceRank(collapsibleRow.source)
  )
}

/** Collapse overlapping email-domain and project-registry rows into one work experience for display. */
export function groupMemberOrganizations<T extends IMemberOrganization>(rows: T[]): T[] {
  const collapsibleRows = rows.filter(
    (row): row is T & { id: string } => !!row.id && isCollapsibleMemberOrganization(row),
  )
  const hiddenCollapsibleIds = new Set<string>()
  const collapsibleParentDisplayId = new Map<string, string>()
  const displayGroups = new Map<string, { displayRow: T & { id: string }; groupedRows: T[] }>()

  for (const collapsibleRow of collapsibleRows) {
    const overlappingDisplayRows = rows.filter(
      (row): row is T & { id: string } =>
        !!row.id &&
        row.id !== collapsibleRow.id &&
        memberOrganizationsOverlap(collapsibleRow, row) &&
        canDisplayCollapsibleRow(row, collapsibleRow),
    )

    if (overlappingDisplayRows.length > 0) {
      const displayRow = [...overlappingDisplayRows].sort(compareMemberOrganizationsBySourceRank)[0]
      hiddenCollapsibleIds.add(collapsibleRow.id)
      collapsibleParentDisplayId.set(collapsibleRow.id, displayRow.id)
    }
  }

  const resolveDisplayRowId = (collapsibleRowId: string): string => {
    let displayRowId = collapsibleRowId
    while (collapsibleParentDisplayId.has(displayRowId)) {
      displayRowId = collapsibleParentDisplayId.get(displayRowId)!
    }
    return displayRowId
  }

  for (const collapsibleRow of collapsibleRows) {
    if (hiddenCollapsibleIds.has(collapsibleRow.id)) {
      const displayRowId = resolveDisplayRowId(collapsibleRow.id)
      const displayRow = rows.find(
        (row): row is T & { id: string } => row.id === displayRowId,
      )

      if (displayRow) {
        const existingGroup = displayGroups.get(displayRowId)
        if (existingGroup) {
          existingGroup.groupedRows.push(collapsibleRow)
        } else {
          displayGroups.set(displayRowId, {
            displayRow,
            groupedRows: [collapsibleRow],
          })
        }
      }
    }
  }

  return rows
    .filter((row): row is T & { id: string } => !!row.id && !hiddenCollapsibleIds.has(row.id))
    .map((row) => {
      const group = displayGroups.get(row.id)
      if (!group) {
        return row
      }

      const groupedRows = [group.displayRow, ...group.groupedRows]

      const normalizedStarts = groupedRows
        .map((groupedRow) => normalizeMemberOrganizationDate(groupedRow.dateStart))
        .filter((date): date is string => date !== null)

      const normalizedEnds = groupedRows.map((groupedRow) =>
        normalizeMemberOrganizationDate(groupedRow.dateEnd),
      )

      const sources = new Set<string>()

      for (const groupedRow of groupedRows) {
        if (groupedRow.source) {
          for (const source of groupedRow.source.split(',')) {
            const trimmed = source.trim()
            if (trimmed) {
              sources.add(trimmed)
            }
          }
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

export function toMemberWorkExperience(mo: IMemberRoleWithOrganization) {
  return {
    id: mo.id,
    organizationId: mo.organizationId,
    organizationName: mo.organizationName,
    organizationLogo: mo.organizationLogo,
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
