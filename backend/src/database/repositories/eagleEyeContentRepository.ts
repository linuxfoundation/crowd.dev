import lodash from 'lodash'
import { Op } from 'sequelize'

import { Error404 } from '@crowd/common'
import { EagleEyeContent } from '@crowd/types'

import EagleEyeActionRepository from './eagleEyeActionRepository'
import QueryParser from './filters/queryParser'
import { QueryOutput } from './filters/queryTypes'
import { IRepositoryOptions } from './IRepositoryOptions'
import SequelizeRepository from './sequelizeRepository'

export default class EagleEyeContentRepository {
  static async create(
    data: EagleEyeContent,
    options: IRepositoryOptions,
  ): Promise<EagleEyeContent> {
    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    const record = await options.database.eagleEyeContent.create({
      ...lodash.pick(data, ['platform', 'post', 'url', 'postedAt']),
      tenantId: currentTenant.id,
    })

    if (data.actions) {
      for (const action of data.actions) {
        await EagleEyeActionRepository.createActionForContent(action, record.id, options)
      }
    }

    return this.findById(record.id, options)
  }

  static async update(id, data, options: IRepositoryOptions) {
    const currentUser = SequelizeRepository.getCurrentUser(options)

    const transaction = SequelizeRepository.getTransaction(options)

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    let record = await options.database.eagleEyeContent.findOne({
      where: {
        id,
        tenantId: currentTenant.id,
      },
      transaction,
    })

    if (!record) {
      throw new Error404()
    }

    record = await record.update(
      {
        ...lodash.pick(data, ['platform', 'post', 'postedAt', 'url']),
        updatedById: currentUser.id,
      },
      {
        transaction,
      },
    )

    return this.findById(record.id, options)
  }

  static async findById(id: string, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const include = [
      {
        model: options.database.eagleEyeAction,
        as: 'actions',
      },
    ]

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    const record = await options.database.eagleEyeContent.findOne({
      where: {
        id,
        tenantId: currentTenant.id,
      },
      include,
      transaction,
    })

    if (!record) {
      throw new Error404()
    }

    return this._populateRelations(record)
  }

  static async destroy(id: string, options: IRepositoryOptions): Promise<void> {
    const transaction = SequelizeRepository.getTransaction(options)

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    const record = await options.database.eagleEyeContent.findOne({
      where: {
        id,
        tenantId: currentTenant.id,
      },
      transaction,
    })

    if (record) {
      await record.destroy({
        transaction,
        force: true,
      })
    }
  }

  static async findAndCountAll(
    { advancedFilter = null as any, limit = 0, offset = 0, orderBy = '' },
    options: IRepositoryOptions,
  ) {
    const actionsSequelizeInclude = {
      model: options.database.eagleEyeAction,
      as: 'actions',
      required: true,
      where: {},
      limit: null,
      offset: 0,
    }

    if (advancedFilter && advancedFilter.action) {
      const actionQueryParser = new QueryParser(
        {
          withSegments: false,
        },
        options,
      )

      const parsedActionQuery: QueryOutput = actionQueryParser.parse({
        filter: advancedFilter.action,
        orderBy: 'timestamp_DESC',
        limit: 0,
        offset: 0,
      })

      actionsSequelizeInclude.where = parsedActionQuery.where ?? {}
      delete advancedFilter.action
    }

    const include = [actionsSequelizeInclude]

    const contentParser = new QueryParser(
      {
        withSegments: false,
      },
      options,
    )

    const parsed: QueryOutput = contentParser.parse({
      filter: advancedFilter,
      orderBy: orderBy || ['postedAt_DESC'],
      limit,
      offset,
    })

    const hasActionFilter = Object.keys(actionsSequelizeInclude.where).length !== 0

    let rows = await options.database.eagleEyeContent.findAll({
      include,
      ...(parsed.where ? { where: parsed.where } : {}),
      order: parsed.order,
      limit: hasActionFilter ? null : parsed.limit,
      offset: hasActionFilter ? 0 : parsed.offset,
      transaction: SequelizeRepository.getTransaction(options),
      subQuery: true,
      distinct: true,
    })

    // count query will group by content id and create a response with action counts
    // ie: it returns a payload similar to this
    // [ contentId1: #ofActionsForContent1, contentId2: #ofActionsForContent2 ]
    // To get the content count, we need to get the length of the response.
    const count = (
      await options.database.eagleEyeContent.count({
        include,
        ...(parsed.where ? { where: parsed.where } : {}),
        transaction: SequelizeRepository.getTransaction(options),
        distinct: true,
        group: ['eagleEyeContent.id'],
      })
    ).length

    // If we have an actions filter, we should query again to eager
    // load the all actions on a content because previous query will
    // omit actions that don't match the given action filter
    if (hasActionFilter) {
      rows = await options.database.eagleEyeContent.findAll({
        include: [
          { ...actionsSequelizeInclude, where: {}, limit: null, offset: 0, required: true },
        ],
        where: { id: { [Op.in]: rows.map((i) => i.id) } },
        order: parsed.order,
        transaction: SequelizeRepository.getTransaction(options),
        subQuery: true,
        limit: parsed.limit,
        offset: parsed.offset,
        distinct: true,
      })
    }

    rows = await this._populateRelationsForRows(rows)

    return { rows: rows ?? [], count, limit: parsed.limit, offset: parsed.offset }
  }

  static async findByUrl(url: string, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const include = []

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    const record = await options.database.eagleEyeContent.findOne({
      where: {
        url,
        tenantId: currentTenant.id,
      },
      include,
      transaction,
    })

    if (!record) {
      return null
    }

    return this._populateRelations(record)
  }

  static async _populateRelationsForRows(rows) {
    if (!rows) {
      return rows
    }

    return Promise.all(rows.map((record) => this._populateRelations(record)))
  }

  static async _populateRelations(record) {
    if (!record) {
      return record
    }

    return record.get({ plain: true })
  }
}
