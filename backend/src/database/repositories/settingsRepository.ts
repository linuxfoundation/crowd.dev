import _get from 'lodash/get'

import SegmentService from '../../services/segmentService'

import { IRepositoryOptions } from './IRepositoryOptions'
import SequelizeRepository from './sequelizeRepository'

export default class SettingsRepository {
  static async findOrCreateDefault(defaults, options: IRepositoryOptions) {
    const currentUser = SequelizeRepository.getCurrentUser(options)

    const tenant = SequelizeRepository.getCurrentTenant(options)
    const [settings] = await options.database.settings.findOrCreate({
      where: { id: tenant.id, tenantId: tenant.id },
      defaults: {
        ...defaults,
        id: tenant.id,
        tenantId: tenant.id,
        createdById: currentUser ? currentUser.id : null,
      },
      transaction: SequelizeRepository.getTransaction(options),
    })

    return this._populateRelations(settings, options)
  }

  static async save(data, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const currentUser = SequelizeRepository.getCurrentUser(options)

    const tenant = SequelizeRepository.getCurrentTenant(options)

    data.backgroundImageUrl = _get(data, 'backgroundImages[0].downloadUrl', null)
    data.logoUrl = _get(data, 'logos[0].downloadUrl', null)
    if (
      typeof data.slackWebHook !== 'string' ||
      (typeof data.slackWebHook === 'string' && !data.slackWebHook?.startsWith('https://'))
    ) {
      data.slackWebHook = undefined
    }

    const [settings] = await options.database.settings.findOrCreate({
      where: { id: tenant.id, tenantId: tenant.id },
      defaults: {
        ...data,
        id: tenant.id,
        tenantId: tenant.id,
        createdById: currentUser ? currentUser.id : null,
      },
      transaction,
    })

    await settings.update(data, {
      transaction,
    })

    return this._populateRelations(settings, options)
  }

  static async getTenantSettings(tenantId: string, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const settings = await options.database.settings.findOne({
      where: { tenantId },
      transaction,
    })

    return settings
  }

  static async _populateRelations(record, options: IRepositoryOptions) {
    if (!record) {
      return record
    }

    const activityTypes = await SegmentService.getTenantActivityTypes(options.currentSegments)

    const settings = record.get({ plain: true })

    settings.activityTypes = activityTypes
    settings.slackWebHook = !!settings.slackWebHook

    return settings
  }
}
