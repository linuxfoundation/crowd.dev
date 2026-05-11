import { queryActivities } from '@crowd/data-access-layer'
import { LoggerBase } from '@crowd/logging'
import { IMemberIdentity, IntegrationResultType, SegmentData } from '@crowd/types'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { getDataSinkWorkerEmitter } from '@/serverless/utils/queueService'

import ActivityRepository from '../database/repositories/activityRepository'
import SegmentRepository from '../database/repositories/segmentRepository'
import {
  UsernameIdentities,
  mapUsernameToIdentities,
} from '../database/repositories/types/memberTypes'

import { IServiceOptions } from './IServiceOptions'
import SegmentService from './segmentService'

export default class ActivityService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  async createWithMember(data) {
    const logger = this.options.log
    const dataSinkWorkerEmitter = await getDataSinkWorkerEmitter()

    try {
      data.member.username = mapUsernameToIdentities(data.member.username, data.platform)

      if (!data.username) {
        data.username = data.member.username[data.platform][0].value
      }

      logger.trace(
        { type: data.type, platform: data.platform, username: data.username },
        'Processing activity with member!',
      )

      data.member.identities = ActivityService.processMemberIdentities(data.member, data.platform)

      // prepare objectMember for dataSinkWorker
      if (data.objectMember) {
        data.objectMember.username = mapUsernameToIdentities(
          data.objectMember.username,
          data.platform,
        )

        if (!data.objectMember.username[data.platform]) {
          throw new Error(`objectMember username for ${data.platform} is missing!`)
        }

        data.objectMemberUsername = data.objectMember.username[data.platform][0].value
        data.objectMember.identities = ActivityService.processMemberIdentities(
          data.objectMember,
          data.platform,
        )
      }

      if (data.member.organizations) {
        data.member.organizations.forEach((org) => {
          org.identities = [
            {
              name: org.name || org.website,
              platform: data.platform,
            },
          ]
        })
      }

      const resultId = await ActivityRepository.createResults(
        {
          type: IntegrationResultType.ACTIVITY,
          data,
        },
        this.options,
      )

      logger.trace(
        { type: data.type, platform: data.platform, username: data.username, processedData: data },
        'Sending activity with member to data-sink-worker!',
      )

      await dataSinkWorkerEmitter.triggerResultProcessing(resultId, resultId, true)
    } catch (error) {
      this.log.error(error, 'Error during activity create with member!')
      throw error
    }
  }

  async findActivityTypes(segments?: string[]) {
    const segmentService = new SegmentService(this.options)

    let subprojects: SegmentData[]

    if (!segments || segments.length === 0) {
      subprojects = await segmentService.getTenantSubprojects()
    } else {
      subprojects = await segmentService.getSegmentSubprojects(segments)
    }

    return SegmentService.getTenantActivityTypes(subprojects)
  }

  async findActivityChannels(segments?: string[]) {
    const segmentService = new SegmentService(this.options)

    let subprojects: SegmentData[]

    if (!segments || segments.length === 0) {
      subprojects = await segmentService.getTenantSubprojects()
    } else {
      subprojects = await segmentService.getSegmentSubprojects(segments)
    }

    return SegmentService.getTenantActivityChannels(
      subprojects.map((s) => s.id),
      this.options,
    )
  }

  async query(data) {
    const filter = data.filter
    const orderBy = Array.isArray(data.orderBy) ? data.orderBy : [data.orderBy]
    const limit = data.limit
    const offset = data.offset
    const countOnly = data.countOnly ?? false

    const segmentIds = SequelizeRepository.getSegmentIds(this.options)
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    const activitiyTypes = SegmentRepository.getActivityTypes(this.options)

    const page = await queryActivities(
      {
        segmentIds,
        filter,
        orderBy,
        limit,
        offset,
        countOnly,
      },
      qx,
      activitiyTypes,
    )

    return page
  }

  static processMemberIdentities(
    member: {
      username: UsernameIdentities
      emails: string[]
    },
    platform: string,
  ): IMemberIdentity[] {
    const identities = []

    if (member.username) {
      Object.keys(member.username).forEach((platform) => {
        identities.push({
          platform,
          value: member.username[platform][0].value,
          type: member.username[platform][0].type,
          verified: true,
        })
      })
    }

    if (member.emails) {
      member.emails.forEach((email) => {
        identities.push({
          platform,
          value: email,
          type: 'email',
          verified: true,
        })
      })
    }

    return identities
  }
}
