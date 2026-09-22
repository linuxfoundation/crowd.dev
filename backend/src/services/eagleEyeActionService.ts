import { Error404 } from '@crowd/common'
import { LoggerBase } from '@crowd/logging'
import { EagleEyeAction, EagleEyeActionType } from '@crowd/types'

import EagleEyeActionRepository from '../database/repositories/eagleEyeActionRepository'
import EagleEyeContentRepository from '../database/repositories/eagleEyeContentRepository'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import track from '../segment/track'
import { IServiceOptions } from './IServiceOptions'

export default class EagleEyeActionService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  async create(data: EagleEyeAction, contentId: string): Promise<EagleEyeAction | null> {
    const transaction = await SequelizeRepository.createTransaction(this.options)

    // find content
    const content = await EagleEyeContentRepository.findById(contentId, {
      ...this.options,
      transaction,
    })

    if (!content) {
      throw new Error404(this.options.language, 'errors.eagleEye.contentNotFound')
    }

    // Tracking here so we have access to url and platform
    track(
      `Eagle Eye post ${data.type === EagleEyeActionType.BOOKMARK ? 'bookmarked' : 'voted'}`,
      {
        type: data.type,
        url: content.url,
        platform: content.platform,
        action: 'create',
      },
      { ...this.options },
    )

    const existingUserActions: EagleEyeAction[] = content.actions.filter(
      (a) => a.actionById === this.options.currentUser.id,
    )

    const existingUserActionTypes = existingUserActions.map((a) => a.type)

    try {
      // check if already bookmarked - if yes ignore the new action and return existing
      if (
        data.type === EagleEyeActionType.BOOKMARK &&
        existingUserActionTypes.includes(EagleEyeActionType.BOOKMARK)
      ) {
        return existingUserActions.find((a) => a.type === EagleEyeActionType.BOOKMARK)
      }

      // thumbs up and down should be mutually exclusive
      if (
        data.type === EagleEyeActionType.THUMBS_DOWN &&
        existingUserActionTypes.includes(EagleEyeActionType.THUMBS_UP)
      ) {
        await EagleEyeActionRepository.removeActionFromContent(
          EagleEyeActionType.THUMBS_UP,
          contentId,
          {
            ...this.options,
            transaction,
          },
        )
      } else if (
        data.type === EagleEyeActionType.THUMBS_UP &&
        existingUserActionTypes.includes(EagleEyeActionType.THUMBS_DOWN)
      ) {
        await EagleEyeActionRepository.removeActionFromContent(
          EagleEyeActionType.THUMBS_DOWN,
          contentId,
          {
            ...this.options,
            transaction,
          },
        )
      }

      // add new action
      const record = await EagleEyeActionRepository.createActionForContent(data, contentId, {
        ...this.options,
        transaction,
      })

      await SequelizeRepository.commitTransaction(transaction)

      return record
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)

      SequelizeRepository.handleUniqueFieldError(error, this.options.language, 'EagleEyeContent')

      throw error
    }
  }

  async destroy(id: string) {
    const action = await EagleEyeActionRepository.findById(id, this.options)

    const contentId = action.contentId

    await EagleEyeActionRepository.destroy(id, this.options)

    // find content
    const content = await EagleEyeContentRepository.findById(contentId, this.options)

    // if content no longer has any actions attached to it, also delete the content
    if (content.actions.length === 0) {
      await EagleEyeContentRepository.destroy(contentId, this.options)
    }

    // Tracking here so we have access to url and platform
    track(
      `Eagle Eye post ${action.type === EagleEyeActionType.BOOKMARK ? 'bookmarked' : 'voted'}`,
      {
        type: action.type,
        url: content.url,
        platform: content.platform,
        action: 'destroy',
      },
      { ...this.options },
    )
  }
}
