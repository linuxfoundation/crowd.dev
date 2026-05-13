import axios, { AxiosRequestConfig } from 'axios'

import { PlatformType } from '@crowd/types'

import { IGenerateStreamsContext, IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'

import { handleLinkedinError } from './handleError'
import { ILinkedInPostReaction, IPaginatedResponse } from './types'

export const getPostReactions = async (
  nangoId: string,
  postId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
  start?: number,
  lookBackUntilTs?: number,
): Promise<IPaginatedResponse<ILinkedInPostReaction>> => {
  const config: AxiosRequestConfig<unknown> = {
    method: 'get',
    url: `https://api.linkedin.com/rest/reactions/(entity:${encodeURIComponent(postId)})`,
    params: {
      q: 'entity',
      count: 20,
      sort: 'REVERSE_CHRONOLOGICAL',
      start,
    },
    headers: {
      'X-Restli-Protocol-Version': '2.0.0',
      'LinkedIn-Version': '202601',
    },
  }

  try {
    ctx.log.debug({ nangoId, postId, start }, 'Fetching post reactions!')

    // Get an access token from Nango
    const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)
    config.params.oauth2_access_token = accessToken

    const response = (await axios(config)).data

    let stop = false

    const elements: ILinkedInPostReaction[] = response.elements.map((e) => {
      if (lookBackUntilTs && e.created.time <= lookBackUntilTs) {
        stop = true
      }

      return {
        authorUrn: e.created.actor,
        reaction: (e.reactionType as string).toLowerCase(),
        timestamp: e.created.time,
      }
    })

    if (stop) {
      return {
        elements,
      }
    }

    if (response.paging.links.find((l) => l.rel === 'next')) {
      return {
        elements,
        start: response.paging.start + response.paging.count,
      }
    }

    return {
      elements,
    }
  } catch (err) {
    const newErr = handleLinkedinError(err, config, { nangoId, postId, start }, ctx.log)
    throw newErr
  }
}

export const getAllPostReactions = async (
  nangoId: string,
  postId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
  lookBackUntilTs?: number,
): Promise<ILinkedInPostReaction[]> => {
  const elements = []

  let response = await getPostReactions(nangoId, postId, ctx, undefined, lookBackUntilTs)
  elements.push(...response.elements)
  while (response.start !== undefined) {
    response = await getPostReactions(nangoId, postId, ctx, response.start, lookBackUntilTs)
    elements.push(...response.elements)
  }

  return elements
}
