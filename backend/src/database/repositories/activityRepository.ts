import lodash from 'lodash'
import sanitizeHtml from 'sanitize-html'
import { QueryTypes } from 'sequelize'

import { Error400, Error404 } from '@crowd/common'
import {
  IQueryActivitiesParameters,
  insertActivities,
  queryActivities,
} from '@crowd/data-access-layer'
import { ActivityDisplayService } from '@crowd/integrations'
import { IIntegrationResult, IntegrationResultState } from '@crowd/types'

import { QUEUE_CLIENT } from '@/serverless/utils/queueService'

import { IRepositoryOptions } from './IRepositoryOptions'
import SegmentRepository from './segmentRepository'
import SequelizeRepository from './sequelizeRepository'

class ActivityRepository {
  static async create(data, options: IRepositoryOptions) {
    const currentUser = SequelizeRepository.getCurrentUser(options)

    const segment = SequelizeRepository.getStrictlySingleActiveSegment(options)

    // Data and body will be displayed as HTML. We need to sanitize them.
    if (data.body) {
      data.body = sanitizeHtml(data.body).trim()
    }

    if (data.title) {
      data.title = sanitizeHtml(data.title).trim()
    }

    if (data.sentiment) {
      this._validateSentiment(data.sentiment)
    }

    // type and platform to lowercase
    if (data.type) {
      data.type = data.type.toLowerCase()
    }
    if (data.platform) {
      data.platform = data.platform.toLowerCase()
    }

    const ids = await insertActivities(QUEUE_CLIENT(), [
      {
        type: data.type,
        timestamp: data.timestamp,
        score: data.score,
        parentId: data.parent || undefined,
        sourceId: data.sourceId,
        sourceParentId: data.sourceParentId || undefined,
        segmentId: segment.id,
        memberId: data.member || undefined,
        username: data.username,
        objectMemberId: data.objectMember || undefined,
        objectMemberUsername: data.objectMemberUsername,
        sentiment: data.sentiment,
        attributes: data.attributes,
        body: data.body,
        title: data.title,
        channel: data.channel,
        url: data.url,
        organizationId: data.organizationId || undefined,
        platform: data.platform,
        conversationId: data.conversationId || undefined,
        createdById: currentUser.id,
        updatedById: currentUser.id,
      },
    ])

    if (ids.length !== 1) {
      throw new Error('Activity was not created!')
    }

    const record = await this.findById(ids[0], options)

    return record
  }

  /**
   * Check whether sentiment data is valid
   * @param sentimentData Object: {positive: number, negative: number, mixed: number, neutral: number, sentiment: 'positive' | 'negative' | 'mixed' | 'neutral'}
   */
  static _validateSentiment(sentimentData) {
    if (!lodash.isEmpty(sentimentData)) {
      const moods = ['positive', 'negative', 'mixed', 'neutral']
      for (const prop of moods) {
        if (typeof sentimentData[prop] !== 'number') {
          throw new Error400('en', 'activity.error.sentiment.mood')
        }
      }
      if (!moods.includes(sentimentData.label)) {
        throw new Error400('en', 'activity.error.sentiment.label')
      }
      if (typeof sentimentData.sentiment !== 'number') {
        throw new Error('activity.error.sentiment.sentiment')
      }
    }
  }

  static async findById(id: string, options: IRepositoryOptions, loadChildren = true) {
    const segmentIds = SequelizeRepository.getSegmentIds(options)

    const qx = SequelizeRepository.getQueryExecutor(options)
    const activityTypes = SegmentRepository.getActivityTypes(options)

    const results = await queryActivities(
      {
        filter: {
          and: [{ id: { eq: id } }],
        },
        segmentIds,
        limit: 1,
      },
      qx,
      activityTypes,
    )

    if (results.rows.length === 0) {
      throw new Error404(`Activity with id ${id} is not found!`)
    }

    if (loadChildren) {
      return this._populateRelations(results.rows[0], options)
    }

    return this._populateRelations(results.rows[0], options)
  }

  /**
   * Find a record in the database given a query.
   * @param query Query to find by
   * @param options Repository options
   * @returns The found record. Null if none is found.
   */
  static async findOne(
    arg: IQueryActivitiesParameters,
    options: IRepositoryOptions,
  ): Promise<any | null> {
    const segmentIds = SequelizeRepository.getSegmentIds(options)

    const qx = SequelizeRepository.getQueryExecutor(options)
    const activityTypes = SegmentRepository.getActivityTypes(options)

    arg.limit = 1
    arg.segmentIds = segmentIds
    arg.groupBy = null

    const results = await queryActivities(arg, qx, activityTypes)

    if (results.rows.length === 0) {
      return null
    }

    return this._populateRelations(results.rows[0], options)
  }

  static async count(filter, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    return options.database.activity.count({
      where: {
        ...filter,
        segmentId: SequelizeRepository.getSegmentIds(options),
      },
      transaction,
    })
  }

  public static ACTIVITY_QUERY_FILTER_COLUMN_MAP: Map<string, string> = new Map([
    ['isTeamMember', "coalesce((m.attributes -> 'isTeamMember' -> 'default')::boolean, false)"],
    ['isBot', "coalesce((m.attributes -> 'isBot' -> 'default')::boolean, false)"],
    ['platform', 'a.platform'],
    ['type', 'a.type'],
    ['channel', 'a.channel'],
    ['timestamp', 'a.timestamp'],
    ['memberId', 'a."memberId"'],
    ['organizationId', 'a."organizationId"'],
    ['conversationId', 'a."conversationId"'],
    ['sentiment', 'a."sentimentMood"'],
  ])

  static async createResults(result: IIntegrationResult, options: IRepositoryOptions) {
    const tenant = SequelizeRepository.getCurrentTenant(options)

    const segment = SequelizeRepository.getStrictlySingleActiveSegment(options)

    const seq = SequelizeRepository.getSequelize(options)

    result.segmentId = segment.id

    const results = await seq.query(
      `
      insert into integration.results(state, data, "tenantId")
      values(:state, :data, :tenantId)
      returning id;
      `,
      {
        replacements: {
          tenantId: tenant.id,
          state: IntegrationResultState.PENDING,
          data: JSON.stringify(result),
        },
        type: QueryTypes.INSERT,
      },
    )

    return results[0][0].id
  }

  static async _populateRelationsForRows(rows, options: IRepositoryOptions) {
    if (!rows) {
      return rows
    }

    return Promise.all(rows.map((record) => this._populateRelations(record, options)))
  }

  static async _populateRelations(record, options: IRepositoryOptions) {
    if (!record) {
      return record
    }

    const output = record.get({ plain: true })

    output.display = ActivityDisplayService.getDisplayOptions(
      record,
      SegmentRepository.getActivityTypes(options),
    )

    if (output.parent) {
      output.parent.display = ActivityDisplayService.getDisplayOptions(
        output.parent,
        SegmentRepository.getActivityTypes(options),
      )
    }

    return output
  }
}

export default ActivityRepository
