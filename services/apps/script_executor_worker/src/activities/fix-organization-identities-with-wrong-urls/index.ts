import { markOrganizationsChanged } from '@crowd/common_services'
import { hasLfxMembership } from '@crowd/data-access-layer/src/lfx_memberships'
import OrganizationRepo from '@crowd/data-access-layer/src/old/apps/script_executor_worker/organization.repo'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { IOrganizationIdentity } from '@crowd/types'

import { svc } from '../../main'

export async function getOrgIdentitiesWithInvalidUrls(
  limit: number,
): Promise<IOrganizationIdentity[]> {
  let organizations: IOrganizationIdentity[] = []

  try {
    const repo = new OrganizationRepo(svc.postgres.reader.connection(), svc.log)
    organizations = await repo.getOrgIdentitiesWithInvalidUrls(limit)
  } catch (err) {
    throw new Error(err)
  }

  return organizations
}

export async function findOrganizationIdentity(
  platform: string,
  value: string,
  type: string,
  verified: boolean,
): Promise<IOrganizationIdentity[]> {
  let orgIdentity: IOrganizationIdentity[]

  try {
    const repo = new OrganizationRepo(svc.postgres.reader.connection(), svc.log)
    orgIdentity = await repo.findOrganizationIdentity(platform, value, type, verified)
  } catch (err) {
    throw new Error(err)
  }

  return orgIdentity
}

export async function updateOrganizationIdentity(
  orgId: string,
  platform: string,
  newValue: string,
  oldValue: string,
  type: string,
  verified: boolean,
): Promise<void> {
  try {
    const repo = new OrganizationRepo(svc.postgres.writer.connection(), svc.log)
    await repo.updateOrganizationIdentity(orgId, platform, newValue, oldValue, type, verified)
    await markOrganizationsChanged(svc.redis, [orgId], svc.log)
  } catch (err) {
    throw new Error(err)
  }
}

export async function deleteOrganizationIdentity(
  orgId: string,
  platform: string,
  type: string,
  value: string,
  verified: boolean,
): Promise<void> {
  try {
    const repo = new OrganizationRepo(svc.postgres.writer.connection(), svc.log)
    await repo.deleteOrganizationIdentity(orgId, platform, type, value, verified)
    await markOrganizationsChanged(svc.redis, [orgId], svc.log)
  } catch (err) {
    throw new Error(err)
  }
}

export async function isLfxMember(organizationId: string): Promise<boolean> {
  let hasMembership: boolean

  try {
    const qx = pgpQx(svc.postgres.reader.connection())
    hasMembership = await hasLfxMembership(qx, { organizationId })
  } catch (err) {
    throw new Error(err)
  }

  return hasMembership
}
