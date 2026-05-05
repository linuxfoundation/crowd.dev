import {
  IMemberOrganization,
  MemberOrgDate,
  MemberOrgStintChange,
  MemberRoleUnmergeStrategy,
} from '@crowd/types'

function roleKey(
  role: IMemberOrganization,
  strategy: MemberRoleUnmergeStrategy,
): string | undefined {
  if (strategy === MemberRoleUnmergeStrategy.SAME_MEMBER) {
    return role.organizationId
  }
  return role.memberId
}

function roleExistsInArray(
  role: IMemberOrganization,
  roles: IMemberOrganization[],
  strategy: MemberRoleUnmergeStrategy,
): boolean {
  const key = roleKey(role, strategy)
  return roles.some(
    (r) =>
      roleKey(r, strategy) === key &&
      r.title === role.title &&
      r.dateStart === role.dateStart &&
      r.dateEnd === role.dateEnd,
  )
}

export function rolesIntersect(
  roleA: IMemberOrganization,
  roleB: IMemberOrganization,
  strategy: MemberRoleUnmergeStrategy,
): boolean {
  if (roleKey(roleA, strategy) !== roleKey(roleB, strategy) || roleA.title !== roleB.title) {
    return false
  }

  const startA = new Date(roleA.dateStart).getTime()
  const endA = new Date(roleA.dateEnd).getTime()
  const startB = new Date(roleB.dateStart).getTime()
  const endB = new Date(roleB.dateEnd).getTime()

  return (
    (startA < startB && endA > startB) ||
    (startB < startA && endB > startA) ||
    (startA < startB && endA > endB) ||
    (startB < startA && endB > endA)
  )
}

export function unmergeRoles(
  mergedRoles: IMemberOrganization[],
  primaryBackupRoles: IMemberOrganization[],
  secondaryBackupRoles: IMemberOrganization[],
  strategy: MemberRoleUnmergeStrategy,
): IMemberOrganization[] {
  const unmergedRoles: IMemberOrganization[] = mergedRoles.filter(
    (role) =>
      role.source === 'ui' ||
      !secondaryBackupRoles.some((r) => roleKey(r, strategy) === roleKey(role, strategy)),
  )

  const editableRoles = mergedRoles.filter(
    (role) =>
      role.source !== 'ui' &&
      secondaryBackupRoles.some((r) => roleKey(r, strategy) === roleKey(role, strategy)),
  )

  for (const secondaryBackupRole of secondaryBackupRoles) {
    const { dateStart, dateEnd } = secondaryBackupRole

    if (dateStart === null && dateEnd === null) {
      if (
        roleExistsInArray(secondaryBackupRole, editableRoles, strategy) &&
        roleExistsInArray(secondaryBackupRole, primaryBackupRoles, strategy)
      ) {
        unmergedRoles.push(secondaryBackupRole)
      }
    } else if (dateStart !== null && dateEnd === null) {
      const currentRoleFromPrimaryBackup = primaryBackupRoles.find(
        (r) =>
          roleKey(r, strategy) === roleKey(secondaryBackupRole, strategy) &&
          r.title === secondaryBackupRole.title &&
          r.dateStart !== null &&
          r.dateEnd === null,
      )
      if (currentRoleFromPrimaryBackup) {
        unmergedRoles.push(currentRoleFromPrimaryBackup)
      }
    } else if (dateStart !== null && dateEnd !== null) {
      if (
        roleExistsInArray(secondaryBackupRole, editableRoles, strategy) &&
        roleExistsInArray(secondaryBackupRole, primaryBackupRoles, strategy)
      ) {
        unmergedRoles.push(secondaryBackupRole)
      } else {
        const intersecting = editableRoles.find((r) =>
          rolesIntersect(secondaryBackupRole, r, strategy),
        )

        if (intersecting) {
          const fromBackup = primaryBackupRoles.find((r) =>
            rolesIntersect(secondaryBackupRole, r, strategy),
          )
          if (fromBackup) {
            unmergedRoles.push(fromBackup)
          }
        }
      }
    }
  }

  return unmergedRoles
}

export const MEMBER_ORG_STINT_CHANGES_QUEUE = 'infer-member-organization-stint-changes:members'
export const MEMBER_ORG_STINT_CHANGES_DATES_PREFIX = 'infer-member-organization-stint-changes:dates'

interface Stint {
  id: string | null
  organizationId: string
  dateStart: string | null
  dateEnd: string | null
  isDirty: boolean
  isNew: boolean
}

type DatedStint = Stint & { dateStart: string; dateEnd: string }

/**
 * Core logic to determine if activity dates should expand existing stints or create new ones
 */
export function inferMemberOrganizationStintChanges(
  memberId: string,
  existingRows: IMemberOrganization[],
  orgDates: MemberOrgDate[],
): MemberOrgStintChange[] {
  const toIso = (v: string | Date) => new Date(v).toISOString().split('T')[0]
  const diff = (a: string, b: string) => Math.abs(Date.parse(b) - Date.parse(a)) / 86_400_000

  // 1. Initialize local state to track modifications and new records
  const stints: Stint[] = existingRows.map((r) => ({
    id: r.id ?? null,
    organizationId: r.organizationId,
    dateStart: r.dateStart ? toIso(r.dateStart) : null,
    dateEnd: r.dateEnd ? toIso(r.dateEnd) : null,
    isDirty: false,
    isNew: false,
  }))

  const sortedDates = [...orgDates].sort((a, b) => a.date.localeCompare(b.date))

  for (const { organizationId, date: targetDate } of sortedDates) {
    const orgStints = stints.filter((s) => s.organizationId === organizationId)

    // 2. Skip if the date is already covered by an existing stint
    if (
      orgStints.some(
        (s) => s.dateStart && s.dateEnd && targetDate >= s.dateStart && targetDate <= s.dateEnd,
      )
    )
      continue

    // 3. Fill undated placeholder only when no dated stint exists yet (Rule 2)
    const dated = orgStints.filter(
      (s): s is DatedStint => s.dateStart !== null && s.dateEnd !== null,
    )
    const placeholder = orgStints.find((s) => !s.dateStart && !s.dateEnd)
    if (placeholder && dated.length === 0) {
      placeholder.dateStart = placeholder.dateEnd = targetDate
      placeholder.isDirty = true
      continue
    }

    // 4. Find the closest neighbor stint to see if expansion is possible
    let neighbor: DatedStint | null = null
    let minGap = Infinity

    for (const s of dated) {
      const gap =
        targetDate > s.dateEnd ? diff(s.dateEnd, targetDate) : diff(targetDate, s.dateStart)
      if (gap < minGap) {
        minGap = gap
        neighbor = s
      }
    }

    if (!neighbor) {
      stints.push({
        id: null,
        organizationId,
        dateStart: targetDate,
        dateEnd: targetDate,
        isDirty: true,
        isNew: true,
      })
      continue
    }

    // 5. Only split when another org clearly sits between this stint and the new date.
    // Overlapping orgs are treated as concurrent evidence, not a break in the stint.
    const isForward = targetDate > neighbor.dateEnd
    const hasSeparator = stints.some((s) => {
      if (
        s.organizationId === organizationId ||
        !s.dateStart ||
        !s.dateEnd ||
        diff(s.dateStart, s.dateEnd) < 30
      ) {
        return false
      }

      if (isForward) {
        return (
          (s.dateStart > neighbor.dateEnd && s.dateStart <= targetDate) ||
          (s.dateEnd > neighbor.dateEnd && s.dateEnd <= targetDate)
        )
      }

      return (
        (s.dateStart >= targetDate && s.dateStart < neighbor.dateStart) ||
        (s.dateEnd >= targetDate && s.dateEnd < neighbor.dateStart)
      )
    })

    if (hasSeparator) {
      // 6a. Another org clearly sits in between — start a fresh stint rather than bridging
      stints.push({
        id: null,
        organizationId,
        dateStart: targetDate,
        dateEnd: targetDate,
        isDirty: true,
        isNew: true,
      })
    } else if (isForward && minGap <= 30) {
      // 6b. Forward extension within the debounce window — skip to avoid thrashing dateEnd.
      // Backward extensions are not debounced (rare, only during historical re-ingestion).
      continue
    } else {
      // 6c. Extend the neighbor in the appropriate direction
      if (isForward) neighbor.dateEnd = targetDate
      else neighbor.dateStart = targetDate
      neighbor.isDirty = true
    }
  }

  // 7. Map only modified or new stints back to change objects
  return stints
    .filter((s) => s.isDirty)
    .map((s): MemberOrgStintChange => {
      const payload = {
        memberId,
        organizationId: s.organizationId,
        dateStart: s.dateStart as string,
        dateEnd: s.dateEnd as string,
      }

      if (s.isNew) return { type: 'insert', ...payload }
      return { type: 'update', id: s.id as string, ...payload }
    })
}
