import {
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicies,
} from '@crowd/data-access-layer'
import { DbStore } from '@crowd/data-access-layer/src/database'
import {
  addOrgsToMember,
  addOrgsToSegments,
  findMemberOrganizations,
  findOrCreateOrganization,
} from '@crowd/data-access-layer/src/organizations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { Logger, LoggerBase } from '@crowd/logging'
import { IMemberOrganization, IOrganization, IOrganizationIdSource } from '@crowd/types'

export class OrganizationService extends LoggerBase {
  constructor(
    private readonly store: DbStore,
    parentLog: Logger,
  ) {
    super(parentLog)
  }

  public async findOrCreate(
    source: string,
    integrationId: string,
    data: IOrganization,
  ): Promise<string> {
    const id = await this.store.transactionally(async (txStore) => {
      const qe = dbStoreQx(txStore)
      const id = await findOrCreateOrganization(qe, source, data, integrationId, true)
      return id
    })

    if (!id) {
      throw new Error('Organization not found or created!')
    }

    return id
  }

  public async addToMember(
    segmentIds: string[],
    memberId: string,
    orgs: IOrganizationIdSource[],
  ): Promise<void> {
    const qe = dbStoreQx(this.store)

    await addOrgsToSegments(
      qe,
      segmentIds,
      orgs.map((org) => org.id),
    )

    const newMemberOrgs = await addOrgsToMember(qe, memberId, orgs)

    const blockedOrgIds = await checkOrganizationAffiliationPolicies(
      qe,
      newMemberOrgs.map((mo) => mo.organizationId),
    )
    const overrides = newMemberOrgs
      .filter((mo) => blockedOrgIds.has(mo.organizationId))
      .map((mo) => ({
        memberId,
        memberOrganizationId: mo.memberOrganizationId,
        allowAffiliation: false,
      }))
    if (overrides.length > 0) {
      await changeMemberOrganizationAffiliationOverrides(qe, overrides)
    }
  }

  public async findMemberOrganizations(
    memberId: string,
    organizationId: string,
  ): Promise<IMemberOrganization[]> {
    const qe = dbStoreQx(this.store)

    return findMemberOrganizations(qe, memberId, organizationId)
  }
}
