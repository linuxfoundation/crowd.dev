import { OrganizationField, queryOrgs } from '@crowd/data-access-layer'
import {
  deleteMemberSegmentAffiliations,
  fetchMemberAffiliations,
  insertMemberSegmentAffiliations,
} from '@crowd/data-access-layer/src/member_segment_affiliations'
import { fetchManySegments } from '@crowd/data-access-layer/src/segments'
import { IMemberAffiliation, IOrganization, SegmentData } from '@crowd/types'

import { IRepositoryOptions } from '../IRepositoryOptions'
import SequelizeRepository from '../sequelizeRepository'

class MemberAffiliationsRepository {
  static async list(memberId: string, options: IRepositoryOptions) {
    const transaction = await SequelizeRepository.createTransaction(options)
    try {
      const txOptions = { ...options, transaction }
      const qx = SequelizeRepository.getQueryExecutor(txOptions)

      // Fetch member affiliations
      const affiliations = await fetchMemberAffiliations(qx, memberId)
      const orgIds = affiliations.map((a) => a.organizationId)
      const segmentIds = affiliations.map((a) => a.segmentId)

      // Fetch organizations
      let orgObject: Record<string, IOrganization> = {}
      if (orgIds.length > 0) {
        const organizations = await queryOrgs(qx, {
          filter: {
            [OrganizationField.ID]: {
              in: orgIds,
            },
          },
          fields: [OrganizationField.ID, OrganizationField.DISPLAY_NAME, OrganizationField.LOGO],
        })
        orgObject = organizations.reduce((acc, org) => {
          acc[org.id] = org
          return acc
        }, {})
      }

      // Fetch organizations
      let segmentsObject: Record<string, SegmentData> = {}
      if (segmentIds.length > 0) {
        const segments = await fetchManySegments(qx, segmentIds, 'id, "slug", "name", "parentName"')
        segmentsObject = segments.reduce((acc, seg) => {
          acc[seg.id] = seg
          return acc
        }, {})
      }

      // Map affiliations
      const list = affiliations.map((affiliation) => {
        const org = orgObject[affiliation.organizationId]
        const segment = segmentsObject[affiliation.segmentId]

        return {
          ...affiliation,
          segmentSlug: segment?.slug,
          segmentName: segment?.name,
          segmentParentName: segment?.parentName,
          organizationName: org?.displayName,
          organizationLogo: org?.logo,
        }
      })

      await SequelizeRepository.commitTransaction(transaction)

      return list
    } catch (err) {
      if (transaction) {
        await SequelizeRepository.rollbackTransaction(transaction)
      }
      throw err
    }
  }

  static async upsertMultiple(
    memberId: string,
    data: Partial<IMemberAffiliation>[],
    options: IRepositoryOptions,
  ) {
    const transaction = await SequelizeRepository.createTransaction(options)
    try {
      const txOptions = { ...options, transaction }
      const qx = SequelizeRepository.getQueryExecutor(txOptions)

      // Delete all member affiliations
      await deleteMemberSegmentAffiliations(qx, { memberId })

      if (data?.length > 0) {
        await insertMemberSegmentAffiliations(
          qx,
          data.map((item) => ({
            memberId,
            segmentId: item.segmentId,
            organizationId: item.organizationId,
            dateStart: item.dateStart || null,
            dateEnd: item.dateEnd || null,
          })),
          true,
        )
      }

      await SequelizeRepository.commitTransaction(transaction)

      return await this.list(memberId, options)
    } catch (err) {
      if (transaction) {
        await SequelizeRepository.rollbackTransaction(transaction)
      }
      throw err
    }
  }
}

export default MemberAffiliationsRepository
