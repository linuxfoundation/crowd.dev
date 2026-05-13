import axios, { AxiosRequestConfig } from 'axios'

import { PlatformType } from '@crowd/types'

import { IGenerateStreamsContext, IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'

import { handleLinkedinError } from './handleError'
import { ILinkedInPostComment, IPaginatedResponse } from './types'

export const getPostComments = async (
  nangoId: string,
  postId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
  start?: number,
  lookBackUntilTs?: number,
): Promise<IPaginatedResponse<ILinkedInPostComment>> => {
  const config: AxiosRequestConfig<unknown> = {
    method: 'get',
    url: `https://api.linkedin.com/rest/socialActions/${postId}/comments`,
    params: {
      count: 20,
      start,
    },
    headers: {
      'LinkedIn-Version': 202601,
    },
  }

  try {
    ctx.log.debug({ nangoId, postId, start }, 'Fetching organization comments!')
    // Get an access token from Nango
    const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)
    config.params.oauth2_access_token = accessToken

    const response = (await axios(config)).data

    let stop = false

    const elements = response.elements.map((e) => {
      if (lookBackUntilTs && e.created.time <= lookBackUntilTs) {
        stop = true
      }

      let imageUrl: string | undefined
      if (e.content && e.content.length > 0 && e.content[0].type === 'IMAGE') {
        imageUrl = e.content[0].url
      }

      return {
        authorUrn: e.actor,
        timestamp: e.created.time,
        comment: e.message.text,
        imageUrl,
        urnId: e.$URN,
        objectUrn: e.object,
        childComments: e.commentsSummary?.aggregatedTotalComments || 0,
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

export const getAllPostComments = async (
  nangoId: string,
  postId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
  lookBackUntilTs?: number,
): Promise<ILinkedInPostComment[]> => {
  const elements = []

  let response = await getPostComments(nangoId, postId, ctx, undefined, lookBackUntilTs)
  elements.push(...response.elements)
  while (response.start !== undefined) {
    response = await getPostComments(nangoId, postId, ctx, response.start, lookBackUntilTs)
    elements.push(...response.elements)
  }

  return elements
}
