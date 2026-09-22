import { findManyLfxMemberships } from '../lfx_memberships'
import { OrganizationField, queryOrgs } from '../organizations'
import { QueryExecutor } from '../queryExecutor'
import { MemberField, findMemberById } from './base'
import { fetchMemberIdentities } from './identities'
import { fetchMemberOrganizations } from './organizations'
import { fetchAbsoluteMemberAggregates } from './segments'

export async function fetchMemberProfile(qx: QueryExecutor, memberId: string) {
  const [member, identities, aggregates, memberOrgs] = await Promise.all([
    findMemberById(qx, memberId, [
      MemberField.ID,
      MemberField.DISPLAY_NAME,
      MemberField.ATTRIBUTES,
      MemberField.JOINED_AT,
    ]),
    fetchMemberIdentities(qx, memberId),
    fetchAbsoluteMemberAggregates(qx, memberId),
    fetchMemberOrganizations(qx, memberId),
  ])

  if (!member) {
    return null
  }

  const orgIds = memberOrgs.map((o) => o.organizationId)
  let orgExtraInfo = []
  let lfxMemberships = []

  if (orgIds.length > 0) {
    orgExtraInfo = await queryOrgs(qx, {
      filter: {
        [OrganizationField.ID]: { in: orgIds },
      },
      fields: [OrganizationField.ID, OrganizationField.DISPLAY_NAME, OrganizationField.LOGO],
    })
    lfxMemberships = await findManyLfxMemberships(qx, { organizationIds: orgIds })
  }

  return {
    ...member,
    identities,
    activityCount: aggregates?.activityCount,
    lastActive: aggregates?.lastActive,
    organizations: memberOrgs.map((o) => ({
      ...orgExtraInfo.find((oei) => oei.id === o.organizationId),
      lfxMembership: lfxMemberships.find((lm) => lm.organizationId === o.organizationId),
      memberOrganizations: o,
    })),
  }
}
