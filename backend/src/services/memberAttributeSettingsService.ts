/* eslint-disable no-restricted-globals */
import moment from 'moment'
import { Transaction } from 'sequelize/types'

import { Error400 } from '@crowd/common'
import { IMemberAttribute, MemberAttributeType } from '@crowd/types'

import { AttributeData } from '../database/attributes/attribute'
import MemberAttributeSettingsRepository from '../database/repositories/memberAttributeSettingsRepository'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import {
  MemberAttributeSettingsCreateData,
  MemberAttributeSettingsCriteria,
  MemberAttributeSettingsCriteriaResult,
  MemberAttributeSettingsUpdateData,
} from '../database/repositories/types/memberAttributeSettingsTypes'
import camelCaseNames from '../utils/camelCaseNames'
import { IServiceOptions } from './IServiceOptions'

export default class MemberAttributeSettingsService {
  options: IServiceOptions

  constructor(options) {
    this.options = options
  }

  /**
   * Cherry picks attributes from predefined integration attributes.
   * @param names array of names to cherry pick
   * @param attributes list of attributes to cherry pick from
   * @returns
   */
  static pickAttributes(names: string[], attributes: IMemberAttribute[]): IMemberAttribute[] {
    return attributes.filter((i) => names.includes(i.name))
  }

  static isBoolean(value): boolean {
    return value === true || value === 'true' || value === false || value === 'false'
  }

  static isNumber(value): boolean {
    return (
      (typeof value === 'number' || (typeof value === 'string' && value.trim() !== '')) &&
      !isNaN(value as number)
    )
  }

  static isEmail(value): boolean {
    const emailRegexp = /^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$/
    return (
      MemberAttributeSettingsService.isString(value) && value !== '' && value.match(emailRegexp)
    )
  }

  static isString(value): boolean {
    return typeof value === 'string'
  }

  static isUrl(value): boolean {
    return MemberAttributeSettingsService.isString(value)
  }

  static isDate(value): boolean {
    if (moment(value, moment.ISO_8601).isValid()) {
      return true
    }

    return false
  }

  static isMultiSelect(values, options) {
    // Type must be array
    if (!Array.isArray(values)) {
      return false
    }
    // If empty array, it is valid
    if (values.length === 0) {
      return true
    }
    if (!options) {
      return false
    }
    // All values must be in options
    for (const value of values) {
      if (!options.includes(value)) {
        return false
      }
    }
    return true
  }

  /**
   * Checks the given attribute value against the attribute type.
   * @param value the value to be checked
   * @param type the type value will be checked against
   * @returns
   */
  static isCorrectType(value, type: MemberAttributeType, inputs: any = {}): boolean {
    switch (type) {
      case MemberAttributeType.BOOLEAN:
        return MemberAttributeSettingsService.isBoolean(value)
      case MemberAttributeType.STRING:
        return MemberAttributeSettingsService.isString(value)
      case MemberAttributeType.DATE:
        return MemberAttributeSettingsService.isDate(value)
      case MemberAttributeType.EMAIL:
        return MemberAttributeSettingsService.isEmail(value)
      case MemberAttributeType.URL:
        return MemberAttributeSettingsService.isUrl(value)
      case MemberAttributeType.NUMBER:
        return MemberAttributeSettingsService.isNumber(value)
      case MemberAttributeType.MULTI_SELECT:
        return MemberAttributeSettingsService.isMultiSelect(value, inputs.options)
      case MemberAttributeType.SPECIAL:
        return true
      default:
        return false
    }
  }

  async create(data: MemberAttributeSettingsCreateData): Promise<AttributeData> {
    const transaction = await SequelizeRepository.createTransaction(this.options)

    try {
      data.name = data.name ?? camelCaseNames(data.label)

      const record = await MemberAttributeSettingsRepository.create(data, {
        ...this.options,
        transaction,
      })

      await SequelizeRepository.commitTransaction(transaction)

      return record
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)

      SequelizeRepository.handleUniqueFieldError(
        error,
        this.options.language,
        'memberAttributeSettings',
      )

      throw error
    }
  }

  /**
   * Creates predefined set of attributes in one function call.
   * Useful when creating predefined platform specific attributes that come
   * from the integrations.
   * @param attributes List of attributes
   * @returns created attributes
   */
  async createPredefined(
    attributes: IMemberAttribute[],
    carryTransaction: Transaction = null,
  ): Promise<AttributeData[]> {
    let transaction

    if (carryTransaction) {
      transaction = carryTransaction
    } else {
      transaction = await SequelizeRepository.createTransaction(this.options)
    }

    try {
      const created = []
      for (const attribute of attributes) {
        // check attribute already exists
        const existing = await MemberAttributeSettingsRepository.findAndCountAll(
          { filter: { name: attribute.name } },
          { ...this.options, transaction },
        )
        if (existing.count === 0) {
          created.push(
            await MemberAttributeSettingsRepository.create(attribute, {
              ...this.options,
              transaction,
            }),
          )
        } else {
          created.push(existing.rows[0])
        }
      }

      if (!carryTransaction) {
        await SequelizeRepository.commitTransaction(transaction)
      }

      return created
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)

      SequelizeRepository.handleUniqueFieldError(
        error,
        this.options.language,
        'memberAttributeSettings',
      )

      throw error
    }
  }

  async destroyAll(ids: string[]): Promise<void> {
    const transaction = await SequelizeRepository.createTransaction(this.options)

    try {
      for (const id of ids) {
        await MemberAttributeSettingsRepository.destroy(id, {
          ...this.options,
          transaction,
        })
      }

      await SequelizeRepository.commitTransaction(transaction)
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }
  }

  async update(id: string, data: MemberAttributeSettingsUpdateData): Promise<AttributeData> {
    const transaction = await SequelizeRepository.createTransaction(this.options)

    try {
      const attribute = await MemberAttributeSettingsRepository.findById(id, {
        ...this.options,
        transaction,
      })

      // we're not allowing updating attribute type to some other value
      if (data.type && attribute.type !== data.type) {
        throw new Error400(
          this.options.language,
          'settings.memberAttributes.errors.typesNotMatching',
        )
      }

      // readonly canDelete field can't be updated to some other value
      if (
        (data.canDelete === true || data.canDelete === false) &&
        attribute.canDelete !== data.canDelete
      ) {
        throw new Error400(
          this.options.language,
          'settings.memberAttributes.errors.canDeleteReadonly',
        )
      }

      // not allowing updating name field as well, delete just in case if name != data.name
      delete data.name

      const record = await MemberAttributeSettingsRepository.update(id, data, {
        ...this.options,
        transaction,
      })

      await SequelizeRepository.commitTransaction(transaction)

      return record
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)

      SequelizeRepository.handleUniqueFieldError(
        error,
        this.options.language,
        'memberAttributeSettings',
      )

      throw error
    }
  }

  async findAndCountAll(
    args: MemberAttributeSettingsCriteria,
  ): Promise<MemberAttributeSettingsCriteriaResult> {
    return MemberAttributeSettingsRepository.findAndCountAll(args, this.options)
  }

  async findById(id: string): Promise<AttributeData> {
    return MemberAttributeSettingsRepository.findById(id, this.options)
  }
}
