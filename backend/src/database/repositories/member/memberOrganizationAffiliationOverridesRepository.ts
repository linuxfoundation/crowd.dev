import {
  changeMemberOrganizationAffiliationOverrides,
  fetchMemberOrganizationById,
  findMemberAffiliationOverrides,
  findPrimaryWorkExperiencesOfMember,
} from '@crowd/data-access-layer'
import { deleteMemberSegmentAffiliations } from '@crowd/data-access-layer/src/member_segment_affiliations'
import {
  IChangeAffiliationOverrideData,
  IMemberOrganizationAffiliationOverride,
} from '@crowd/types'

import { IRepositoryOptions } from '../IRepositoryOptions'
import SequelizeRepository from '../sequelizeRepository'

class MemberOrganizationAffiliationOverridesRepository {
  static async changeOverride(data: IChangeAffiliationOverrideData, options: IRepositoryOptions) {
    const qx = SequelizeRepository.getQueryExecutor(options)

    await changeMemberOrganizationAffiliationOverrides(qx, [data])

    const { allowAffiliation, memberId, memberOrganizationId } = data

    if (allowAffiliation === false && memberId && memberOrganizationId) {
      const memberOrganization = await fetchMemberOrganizationById(qx, memberOrganizationId)

      if (memberOrganization?.organizationId) {
        await deleteMemberSegmentAffiliations(qx, {
          memberId,
          organizationId: memberOrganization.organizationId,
        })
      }
    }

    const overrides = await findMemberAffiliationOverrides(qx, data.memberId, [
      data.memberOrganizationId,
    ])

    return overrides[0]
  }

  static async findPrimaryWorkExperiences(
    memberId: string,
    options: IRepositoryOptions,
  ): Promise<IMemberOrganizationAffiliationOverride[]> {
    const qx = SequelizeRepository.getQueryExecutor(options)
    return findPrimaryWorkExperiencesOfMember(qx, memberId)
  }
}

export default MemberOrganizationAffiliationOverridesRepository
