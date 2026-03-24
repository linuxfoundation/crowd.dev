/* eslint-disable no-continue */
import { uniq } from 'lodash'

import { Error400, dateIntersects, groupBy } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import { optionsQx } from '@crowd/data-access-layer'
import { findMaintainerRoles } from '@crowd/data-access-layer/src/maintainers'
import { fetchManySegments } from '@crowd/data-access-layer/src/segments'
import { LoggerBase } from '@crowd/logging'
import {
  IChangeAffiliationOverrideData,
  IMemberAffiliation,
  IMemberOrganizationAffiliationOverride,
} from '@crowd/types'

import MemberAffiliationsRepository from '@/database/repositories/member/memberAffiliationsRepository'
import MemberOrganizationAffiliationOverridesRepository from '@/database/repositories/member/memberOrganizationAffiliationOverridesRepository'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import { IServiceOptions } from '../IServiceOptions'

import MemberOrganizationsService from './memberOrganizationsService'

export default class MemberAffiliationsService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  // Member affiliations list
  async list(memberId: string): Promise<IMemberAffiliation[]> {
    const affiliations: IMemberAffiliation[] = await MemberAffiliationsRepository.list(
      memberId,
      this.options,
    )

    await (async function addMaintainerRoles(options: IServiceOptions, rows: IMemberAffiliation[]) {
      const qx = SequelizeRepository.getQueryExecutor(options)
      const maintainerRoles = await findMaintainerRoles(qx, [memberId])
      const segmentIds = uniq(maintainerRoles.map((m) => m.segmentId))
      const segmentsInfo = await fetchManySegments(qx, segmentIds)

      const groupedMaintainers = groupBy(maintainerRoles, (m) => m.segmentId)
      rows.forEach((row: any) => {
        row.maintainerRoles = (groupedMaintainers.get(row.segmentId) || []).map((role) => {
          const segmentInfo = segmentsInfo.find((s) => s.id === role.segmentId)
          return {
            ...role,
            segmentName: segmentInfo?.name,
          }
        })
      })
    })(this.options, affiliations)

    return affiliations
  }

  // Member multiple identity creation
  async upsertMultiple(
    memberId: string,
    data: Partial<IMemberAffiliation>[],
  ): Promise<IMemberAffiliation[]> {
    return MemberAffiliationsRepository.upsertMultiple(memberId, data, this.options)
  }

  async changeAffiliationOverride(
    data: IChangeAffiliationOverrideData,
  ): Promise<IMemberOrganizationAffiliationOverride> {
    if (data.isPrimaryWorkExperience) {
      const memberOrgService = new MemberOrganizationsService(this.options)
      // check if any other work experience in intersecting date range was marked as primary
      // we don't allow this because "isPrimaryWorkExperience" decides which work exp to pick on date conflicts
      const allWorkExperiencesOfMember = (await memberOrgService.list(data.memberId)).map(
        (mo) => mo.memberOrganizations,
      )

      const currentlyEditedWorkExperience = allWorkExperiencesOfMember.find(
        (w) => w.id === data.memberOrganizationId,
      )

      const primaryWorkExperiencesOfMember = allWorkExperiencesOfMember.filter(
        (w) => w.affiliationOverride.isPrimaryWorkExperience,
      )

      if (currentlyEditedWorkExperience.affiliationOverride.isPrimaryWorkExperience === false) {
        for (const existingPrimaryWorkExp of primaryWorkExperiencesOfMember) {
          if (
            dateIntersects(
              existingPrimaryWorkExp.dateStart as string,
              existingPrimaryWorkExp.dateEnd as string,
              currentlyEditedWorkExperience.dateStart as string,
              currentlyEditedWorkExperience.dateEnd as string,
            )
          ) {
            throw new Error400(
              this.options.language,
              `Date range conflicts with another primary work experience id = ${existingPrimaryWorkExp.id}`,
            )
          }
        }
      }
    }

    const override = await MemberOrganizationAffiliationOverridesRepository.changeOverride(
      data,
      this.options,
    )

    const commonMemberService = new CommonMemberService(
      optionsQx(this.options),
      this.options.temporal,
      this.options.log,
    )
    await commonMemberService.startAffiliationRecalculation(data.memberId, [])
    return override
  }
}
