import axios from 'axios'
import moment from 'moment'

import { Error400 } from '@crowd/common'
import { LoggerBase } from '@crowd/logging'
import {
  EagleEyeAction,
  EagleEyeContent,
  EagleEyePostWithActions,
  EagleEyePublishedDates,
  EagleEyeRawPost,
  EagleEyeSettings,
  PageData,
  QueryData,
} from '@crowd/types'

import { EAGLE_EYE_CONFIG } from '../conf'
import EagleEyeContentRepository from '../database/repositories/eagleEyeContentRepository'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import TenantUserRepository from '../database/repositories/tenantUserRepository'
import track from '../segment/track'
import { IServiceOptions } from './IServiceOptions'

export interface EagleEyeContentUpsertData extends EagleEyeAction {
  content: EagleEyeContent
}

export default class EagleEyeContentService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  /**
   * Create an eagle eye shown content record.
   * @param data Data to a new EagleEyeContent record.
   * @param options Repository options.
   * @returns Created EagleEyeContent record.
   */
  async upsert(data: EagleEyeContent): Promise<EagleEyeContent | null> {
    if (!data.url) {
      throw new Error400(this.options.language, 'errors.eagleEye.urlRequiredWhenUpserting')
    }
    const transaction = await SequelizeRepository.createTransaction(this.options)

    try {
      // find by url
      const existing = await EagleEyeContentRepository.findByUrl(data.url, {
        ...this.options,
        transaction,
      })

      let record

      if (existing) {
        record = await EagleEyeContentRepository.update(existing.id, data, {
          ...this.options,
          transaction,
        })
      } else {
        record = await EagleEyeContentRepository.create(data, {
          ...this.options,
          transaction,
        })
      }

      await SequelizeRepository.commitTransaction(transaction)

      return record
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }
  }

  async findById(id: string): Promise<EagleEyeContent> {
    return EagleEyeContentRepository.findById(id, this.options)
  }

  async query(data: QueryData): Promise<PageData<EagleEyeContent>> {
    const advancedFilter = data.filter
    const orderBy = data.orderBy
    const limit = data.limit
    const offset = data.offset
    return EagleEyeContentRepository.findAndCountAll(
      { advancedFilter, orderBy, limit, offset },
      this.options,
    )
  }

  static trackPostClicked(url: string, platform: string, req: any, source: string = 'app'): void {
    track(
      'Eagle Eye post clicked',
      {
        url,
        platform,
        source,
      },
      { ...req },
    )
  }

  static trackDigestEmailOpened(req: any): void {
    track('Eagle Eye digest opened', {}, { ...req })
  }

  /**
   * Convert a relative string date to a Date. For example, 30 days ago -> 2020-01-01
   * @param date String date. Can be one of EagleEyePublishedDates
   * @returns The corresponding Date
   */
  static switchDate(date: string, offset = 0) {
    let dateMoment
    switch (date) {
      case EagleEyePublishedDates.LAST_24_HOURS:
        dateMoment = moment().subtract(1, 'days')
        break
      case EagleEyePublishedDates.LAST_7_DAYS:
        dateMoment = moment().subtract(7, 'days')
        break
      case EagleEyePublishedDates.LAST_14_DAYS:
        dateMoment = moment().subtract(14, 'days')
        break
      case EagleEyePublishedDates.LAST_30_DAYS:
        dateMoment = moment().subtract(30, 'days')
        break
      case EagleEyePublishedDates.LAST_90_DAYS:
        dateMoment = moment().subtract(90, 'days')
        break
      default:
        return null
    }
    return dateMoment.subtract(offset, 'days').format('YYYY-MM-DD')
  }

  async search(email = false) {
    const eagleEyeSettings: EagleEyeSettings = (
      await TenantUserRepository.findByTenantAndUser(
        this.options.currentTenant.id,
        this.options.currentUser.id,
        this.options,
      )
    ).settings.eagleEye

    if (!eagleEyeSettings.onboarded) {
      throw new Error400(this.options.language, 'errors.eagleEye.notOnboarded')
    }

    const feedSettings = email ? eagleEyeSettings.emailDigest.feed : eagleEyeSettings.feed

    const keywords = feedSettings.keywords ? feedSettings.keywords.join(',') : ''
    const exactKeywords = feedSettings.exactKeywords ? feedSettings.exactKeywords.join(',') : ''
    const excludedKeywords = feedSettings.excludedKeywords
      ? feedSettings.excludedKeywords.join(',')
      : ''

    const afterDate = EagleEyeContentService.switchDate(feedSettings.publishedDate)

    const config = {
      method: 'get',
      maxBodyLength: Infinity,
      url: `${EAGLE_EYE_CONFIG.url}`,
      params: {
        platforms: feedSettings.platforms.join(','),
        keywords,
        exact_keywords: exactKeywords,
        exclude_keywords: excludedKeywords,
        after_date: afterDate,
      },
      headers: {
        Authorization: `Bearer ${EAGLE_EYE_CONFIG.apiKey}`,
      },
    }

    let response

    try {
      response = await axios(config)
    } catch (error) {
      this.log.error('Error while fetching eagle eye content', error, config)
      return []
    }

    const interacted = (
      await this.query({
        filter: {
          postedAt: { gt: EagleEyeContentService.switchDate(feedSettings.publishedDate, 90) },
        },
      })
    ).rows

    const interactedMap = {}

    for (const item of interacted) {
      interactedMap[item.url] = item
    }

    const out: EagleEyePostWithActions[] = []
    for (const item of response.data as EagleEyeRawPost[]) {
      const post = {
        description: item.description,
        thumbnail: item.thumbnail,
        title: item.title,
      }
      out.push({
        url: item.url,
        postedAt: item.date,
        post,
        platform: item.platform,
        actions: interactedMap[item.url] ? interactedMap[item.url].actions : [],
      })
    }

    return out
  }

  static async reply(title, description) {
    const config = {
      method: 'get',
      maxBodyLength: Infinity,
      url: `${EAGLE_EYE_CONFIG.url}/reply`,
      params: {
        title,
        description,
      },
      headers: {
        Authorization: `Bearer ${EAGLE_EYE_CONFIG.apiKey}`,
      },
    }

    const response = await axios(config)
    return {
      reply: response.data,
    }
  }
}
