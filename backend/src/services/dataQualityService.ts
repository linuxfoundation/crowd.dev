import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { IDataQualityParams, IDataQualityType } from '@/types/data-quality/data-quality-filters'
import {
  fetchMembersWithConflictingWorkExperiences,
  fetchMembersWithMissingInfoOnWorkExperience,
  fetchMembersWithMissingPeriodOnWorkExperience,
  fetchMembersWithTooManyEmails,
  fetchMembersWithTooManyIdentities,
  fetchMembersWithTooManyIdentitiesPerPlatform,
  fetchMembersWithoutWorkExperience,
} from '@crowd/data-access-layer/src/data-quality'
import { LoggerBase } from '@crowd/logging'

import { IServiceOptions } from './IServiceOptions'

export default class DataQualityService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  /**
   * Finds issues related to member data quality based on the specified type.
   *
   * @param {IDataQualityParams} params - The parameters for finding member issues, including the type of issue, limit, and offset.
   * @param {string} segmentId - The ID of the segment where the members belong.
   * @return {Promise<Array>} A promise that resolves to an array of members with the specified data quality issues.
   */
  async findMemberIssues(params: IDataQualityParams, segmentId: string) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)

    const limit = params.limit || 10
    const offset = params.offset || 0

    switch (params.type) {
      case IDataQualityType.NO_WORK_EXPERIENCE:
        return fetchMembersWithoutWorkExperience(qx, limit, offset, segmentId)
      case IDataQualityType.TOO_MANY_IDENTITIES:
        return fetchMembersWithTooManyIdentities(qx, 30, limit, offset, segmentId)
      case IDataQualityType.TOO_MANY_IDENTITIES_PER_PLATFORM:
        return fetchMembersWithTooManyIdentitiesPerPlatform(qx, 1, limit, offset, segmentId)
      case IDataQualityType.TOO_MANY_EMAILS:
        return fetchMembersWithTooManyEmails(qx, 5, limit, offset, segmentId)
      case IDataQualityType.WORK_EXPERIENCE_MISSING_INFO:
        return fetchMembersWithMissingInfoOnWorkExperience(qx, limit, offset, segmentId)
      case IDataQualityType.WORK_EXPERIENCE_MISSING_PERIOD:
        return fetchMembersWithMissingPeriodOnWorkExperience(qx, limit, offset, segmentId)
      case IDataQualityType.CONFLICTING_WORK_EXPERIENCE:
        return fetchMembersWithConflictingWorkExperiences(qx, limit, offset, segmentId)
      default:
        throw new Error(`Unsupported data quality filter type: ${params.type}`)
    }
  }

  // TODO: Implement this method when there are checks available
  // eslint-disable-next-line class-methods-use-this
  async findOrganizationIssues() {
    return Promise.resolve([])
  }
}
