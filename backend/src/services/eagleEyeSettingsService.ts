import lodash from 'lodash'
import moment from 'moment'

import { Error400 } from '@crowd/common'
import { LoggerBase } from '@crowd/logging'
import {
  EagleEyeEmailDigestFrequency,
  EagleEyeEmailDigestSettings,
  EagleEyeFeedSettings,
  EagleEyePlatforms,
  EagleEyePublishedDates,
  EagleEyeSettings,
} from '@crowd/types'

import SequelizeRepository from '../database/repositories/sequelizeRepository'
import TenantUserRepository from '../database/repositories/tenantUserRepository'
import track from '../segment/track'
import { IServiceOptions } from './IServiceOptions'

/* eslint-disable no-case-declarations */

export default class EagleEyeSettingsService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  /**
   * Validate and normalize feed settings.
   * @param data Feed data of type EagleEyeFeedSettings
   * @returns Normalized feed data if the input is valid. Otherwise a 400 Error
   */
  getFeed(data: EagleEyeFeedSettings) {
    // Feed is compulsory
    if (!data) {
      throw new Error400(this.options.language, 'errors.eagleEye.feedSettingsMissing')
    }

    // We need at least one of keywords or exactKeywords
    if (!data.keywords && !data.exactKeywords) {
      throw new Error400(this.options.language, 'errors.eagleEye.keywordsMissing')
    }

    // We need at least one platform
    if (!data.platforms || data.platforms.length === 0) {
      throw new Error400(this.options.language, 'errors.eagleEye.platformMissing')
    }

    // Make sure platforms are in the allowed list
    const platforms = Object.values(EagleEyePlatforms) as string[]
    data.platforms.forEach((platform) => {
      if (!platforms.includes(platform)) {
        throw new Error400(
          this.options.language,
          'errors.eagleEye.platformInvalid',
          platform,
          platforms.join(', '),
        )
      }
    })

    // We need a date. Make sure it's in the allowed list.
    const publishedDates = Object.values(EagleEyePublishedDates) as string[]
    if (publishedDates.indexOf(data.publishedDate) === -1) {
      throw new Error400(
        this.options.language,
        'errors.eagleEye.publishedDateMissing',
        publishedDates.join(', '),
      )
    }

    // Remove any extra fields
    return lodash.pick(data, [
      'keywords',
      'exactKeywords',
      'excludedKeywords',
      'publishedDate',
      'platforms',
    ])
  }

  /**
   * Validate and normalize email digest settings.
   * @param data Email digest settings of type EagleEyeEmailDigestSettings
   * @param feed Standard feed settings of type EagleEyeFeedSettings
   * @returns The normalized email digest settings if the input is valid. Otherwise a 400 Error.
   */
  getEmailDigestSettings(data: EagleEyeEmailDigestSettings, feed: EagleEyeFeedSettings) {
    // If the matchFeedSettings option is toggled, we set the email feed settings to the standard feed settings.
    // Otherwise, we validate and normalize the email feed settings.
    if (!data.matchFeedSettings) {
      data.feed = this.getFeed(data.feed)
    } else {
      data.feed = feed
    }

    // Make sure the email exists and is valid
    const emailRegex =
      /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
    if (!emailRegex.test(data.email)) {
      throw new Error400(this.options.language, 'errors.eagleEye.emailInvalid')
    }

    // get next email trigger time
    data.nextEmailAt = EagleEyeSettingsService.getNextEmailDigestDate(data)

    // Make sure the time exists and is valid
    const timeRegex = /^([0-1]?[0-9]|2[0-3]):[0-5][0-9]?$/
    if (!timeRegex.test(data.time)) {
      throw new Error400(this.options.language, 'errors.eagleEye.timeInvalid')
    }

    // Remove any extra fields
    return lodash.pick(data, [
      'email',
      'frequency',
      'time',
      'matchFeedSettings',
      'feed',
      'nextEmailAt',
    ])
  }

  /**
   * Finds the next email digest send time using the frequency set by the user
   * eagleEyeSettings.emailDigest.nextEmailAt will be
   * set to actual send time minus 5 minutes.
   * This serves as a buffer for cronjobs - Email crons will fire
   * at every half hour (10:00, 10:30, 11:00,...) to ensure the
   * correct send time set by the user.
   * @param settings
   * @returns next email date as iso string
   */
  static getNextEmailDigestDate(settings: EagleEyeEmailDigestSettings): string {
    const now = moment()

    let nextEmailAt: string = ''

    switch (settings.frequency) {
      case EagleEyeEmailDigestFrequency.DAILY:
        nextEmailAt = moment(settings.time, 'HH:mm').subtract(5, 'minutes').toISOString()

        // if send time has passed for today, set it to next day
        if (now > moment(settings.time, 'HH:mm')) {
          nextEmailAt = moment(nextEmailAt).add(1, 'day').toISOString()
        }
        break
      case EagleEyeEmailDigestFrequency.WEEKLY:
        const [hour, minute] = settings.time.split(':')
        const startOfWeek = moment()
          .startOf('isoWeek')
          .set('hour', parseInt(hour, 10))
          .set('minute', parseInt(minute, 10))
          .subtract(5, 'minutes')

        nextEmailAt = startOfWeek.toISOString()

        // if send time has passed for this week, set it to next week
        if (now > startOfWeek) {
          nextEmailAt = startOfWeek.add(1, 'week').toISOString()
        }
        break
      default:
        throw new Error(`Unknown email digest frequency: ${settings.frequency}`)
    }

    return nextEmailAt
  }

  /**
   * Validate, normalize and update EagleEye settings.
   * @param data Input of type EagleEyeSettings
   * @returns Normalized EagleEyeSettings if the input is valid. Otherwise a 400 Error.
   */
  async update(data: EagleEyeSettings): Promise<EagleEyeSettings> {
    const transaction = await SequelizeRepository.createTransaction(this.options)
    try {
      // At this point onboarded is true always
      data.onboarded = true

      // Validate and normalize feed settings
      data.feed = this.getFeed(data.feed)

      // If an email digest was sent, validate and normalize email digest settings
      // Otherwise, set email digest to false
      if (data.emailDigestActive && data.emailDigest) {
        data.emailDigest = this.getEmailDigestSettings(data.emailDigest, data.feed)
      }

      // Remove any extra fields
      data = lodash.pick(data, [
        'onboarded',
        'feed',
        'emailDigestActive',
        'emailDigest',
        'aiReplies',
      ])

      // Update the user's EagleEye settings
      const tenantUserOut = await TenantUserRepository.updateEagleEyeSettings(
        this.options.currentUser.id,
        data,
        { ...this.options, transaction },
      )

      await SequelizeRepository.commitTransaction(transaction)

      // Track the events in Segment
      const settingsOut: EagleEyeSettings = tenantUserOut.settings.eagleEye

      if (data.emailDigestActive) {
        track(
          'Eagle Eye email settings updated',
          {
            email: settingsOut.emailDigest.email,
            frequency: settingsOut.emailDigest.frequency,
            time: settingsOut.emailDigest.time,
            matchFeedSettings: settingsOut.emailDigest.matchFeedSettings,
            platforms: settingsOut.emailDigest.feed.platforms,
            publishedDate: settingsOut.emailDigest.feed.publishedDate,
            keywords: settingsOut.emailDigest.feed.keywords,
            exactKeywords: settingsOut.emailDigest.feed.exactKeywords,
            excludeKeywords: settingsOut.emailDigest.feed.excludedKeywords,
          },
          { ...this.options },
        )
      } else {
        track(
          'Eagle Eye settings updated',
          {
            onboarded: settingsOut.onboarded,
            emailDigestActive: settingsOut.emailDigestActive,
            platforms: settingsOut.feed.platforms,
            publishedDate: settingsOut.feed.publishedDate,
            keywords: settingsOut.feed.keywords,
            exactKeywords: settingsOut.feed.exactKeywords,
            excludeKeywords: settingsOut.feed.excludedKeywords,
          },
          { ...this.options },
        )
      }

      return tenantUserOut.settings.eagleEye
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)

      SequelizeRepository.handleUniqueFieldError(error, this.options.language, 'EagleEyeContent')

      throw error
    }
  }
}
