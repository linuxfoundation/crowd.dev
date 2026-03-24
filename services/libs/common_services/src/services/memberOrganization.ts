import { IMemberOrganization, MemberRoleUnmergeStrategy } from '@crowd/types'

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
