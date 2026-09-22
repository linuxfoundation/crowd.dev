import Sequelize from 'sequelize'

import { Error400, Error404 } from '@crowd/common'
import { RedisCache } from '@crowd/redis'

import { AttributeData } from '../attributes/attribute'
import SequelizeFilterUtils from '../utils/sequelizeFilterUtils'
import { IRepositoryOptions } from './IRepositoryOptions'
import SequelizeRepository from './sequelizeRepository'
import {
  MemberAttributeSettingsCreateData,
  MemberAttributeSettingsCriteria,
  MemberAttributeSettingsCriteriaResult,
  MemberAttributeSettingsUpdateData,
} from './types/memberAttributeSettingsTypes'

const Op = Sequelize.Op

class MemberAttributeSettingsRepository {
  static async findById(id: string, options: IRepositoryOptions): Promise<AttributeData> {
    const transaction = SequelizeRepository.getTransaction(options)

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    const record = await options.database.memberAttributeSettings.findOne({
      where: {
        id,
        tenantId: currentTenant.id,
      },
      transaction,
    })

    if (!record) {
      throw new Error404()
    }

    return this._populateRelations(record)
  }

  static async _populateRelations(record) {
    if (!record) {
      return record
    }
    const output = record.get({ plain: true })

    return output
  }

  static async create(
    data: MemberAttributeSettingsCreateData,
    options: IRepositoryOptions,
  ): Promise<AttributeData> {
    const currentUser = SequelizeRepository.getCurrentUser(options)

    const tenant = SequelizeRepository.getCurrentTenant(options)

    const transaction = SequelizeRepository.getTransaction(options)

    if (Object.keys(options.database.member.rawAttributes).includes(data.name)) {
      throw new Error400(
        options.language,
        'settings.memberAttributes.errors.reservedField',
        data.name,
      )
    }

    const record = await options.database.memberAttributeSettings.create(
      {
        type: data.type,
        name: data.name,
        label: data.label,
        canDelete: data.canDelete,
        show: data.show,
        options: data.options,
        tenantId: tenant.id,
        createdById: currentUser.id,
        updatedById: currentUser.id,
      },
      {
        transaction,
      },
    )

    const cache = new RedisCache('memberAttributes', options.redis, options.log)

    await cache.delete(tenant.id)

    return this.findById(record.id, options)
  }

  static async update(
    id: string,
    data: MemberAttributeSettingsUpdateData,
    options: IRepositoryOptions,
  ): Promise<AttributeData> {
    const currentUser = SequelizeRepository.getCurrentUser(options)

    const transaction = SequelizeRepository.getTransaction(options)

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    let record = await options.database.memberAttributeSettings.findOne({
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
        ...data,
        updatedById: currentUser.id,
      },
      {
        transaction,
      },
    )

    const cache = new RedisCache('memberAttributes', options.redis, options.log)
    await cache.delete(currentTenant.id)

    return this.findById(record.id, options)
  }

  static async destroy(id: string, options: IRepositoryOptions): Promise<void> {
    const transaction = SequelizeRepository.getTransaction(options)

    const currentTenant = SequelizeRepository.getCurrentTenant(options)

    const record = await options.database.memberAttributeSettings.findOne({
      where: {
        id,
        tenantId: currentTenant.id,
      },
      transaction,
    })

    if (!record) {
      throw new Error404()
    }

    if (record.canDelete) {
      await record.destroy({
        transaction,
      })

      const cache = new RedisCache('memberAttributes', options.redis, options.log)
      await cache.delete(currentTenant.id)
    }
  }

  static async findAndCountAll(
    { filter, limit = 0, offset = 0, orderBy = '' }: MemberAttributeSettingsCriteria,
    options: IRepositoryOptions,
  ): Promise<MemberAttributeSettingsCriteriaResult> {
    const tenant = SequelizeRepository.getCurrentTenant(options)

    const whereAnd: Array<any> = []

    whereAnd.push({
      tenantId: tenant.id,
    })

    if (filter) {
      if (filter.id) {
        whereAnd.push({
          id: SequelizeFilterUtils.uuid(filter.id),
        })
      }

      if (filter.canDelete === true || filter.canDelete === false) {
        whereAnd.push({
          canDelete: filter.canDelete === true,
        })
      }

      if (filter.show === true || filter.show === false) {
        whereAnd.push({
          show: filter.show === true,
        })
      }

      if (filter.type) {
        whereAnd.push({
          type: filter.type,
        })
      }

      if (filter.label) {
        whereAnd.push({
          label: filter.label,
        })
      }

      if (filter.name) {
        whereAnd.push({
          name: filter.name,
        })
      }

      if (filter.createdAtRange) {
        const [start, end] = filter.createdAtRange

        if (start !== undefined && start !== null && start !== '') {
          whereAnd.push({
            createdAt: {
              [Op.gte]: start,
            },
          })
        }

        if (end !== undefined && end !== null && end !== '') {
          whereAnd.push({
            createdAt: {
              [Op.lte]: end,
            },
          })
        }
      }
    }

    const where = { [Op.and]: whereAnd }

    // eslint-disable-next-line prefer-const
    let { rows, count } = await options.database.memberAttributeSettings.findAndCountAll({
      where,
      limit: limit ? Number(limit) : undefined,
      offset: offset ? Number(offset) : undefined,
      order: orderBy ? [orderBy.split('_')] : [['createdAt', 'DESC']],
      transaction: SequelizeRepository.getTransaction(options),
    })

    rows = await this._populateRelationsForRows(rows)
    // TODO add limit and offset
    return { rows, count }
  }

  static async _populateRelationsForRows(rows) {
    if (!rows) {
      return rows
    }

    return Promise.all(rows.map((record) => this._populateRelations(record)))
  }
}

export default MemberAttributeSettingsRepository
