import axios, { AxiosRequestConfig } from 'axios'

import { PlatformType } from '@crowd/types'

import { IGenerateStreamsContext, IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'

import { handleLinkedinError } from './handleError'
import { ILinkedInOrganizationPost, IPaginatedResponse } from './types'

export const getOrganizationPosts = async (
  nangoId: string,
  organization: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
  start?: number,
  lookBackUntilTs?: number,
): Promise<IPaginatedResponse<ILinkedInOrganizationPost>> => {
  const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)

  const config: AxiosRequestConfig<unknown> = {
    method: 'get',
    url: `https://api.linkedin.com/rest/posts`,
    params: {
      author: organization,
      q: 'author',
      count: 10,
      start,
    },
    headers: {
      'LinkedIn-Version': 202601,
      Authorization: `Bearer ${accessToken}`,
    },
  }

  try {
    ctx.log.debug({ nangoId, organization, start }, 'Fetching organization posts!')

    const response = (await axios(config)).data

    let stop = false

    const elements = response.elements
      .filter((e) => e.publishedAt !== undefined)
      .map((e) => {
        if (lookBackUntilTs && e.createdAt <= lookBackUntilTs) {
          stop = true
        }

        return {
          urnId: e.id,
          lifecycleState: e.lifecycleState,
          visibility: e.visibility,
          authorUrn: e.author,
          body: e.commentary,
          originalUrnId: e.reshareContext?.parent,
          timestamp: e.createdAt,
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
    ctx.log.error(err, { nangoId, organization, start }, 'Error while fetching organization posts!')
    if (err.response) {
      ctx.log.error(
        {
          status: err.response.status,
          headers: err.response.headers,
          data: err.response.data,
        },
        'Error response from LinkedIn API',
      )
    }
    const newErr = handleLinkedinError(err, config, { nangoId, organization, start }, ctx.log)
    throw newErr
  }
}

export const getAllOrganizationPosts = async (
  nangoId: string,
  organization: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
  lookBackUntilTs?: number,
): Promise<ILinkedInOrganizationPost[]> => {
  const elements = []

  let response = await getOrganizationPosts(nangoId, organization, ctx, undefined, lookBackUntilTs)
  elements.push(...response.elements)
  while (response.start !== undefined) {
    response = await getOrganizationPosts(
      nangoId,
      organization,
      ctx,
      response.start,
      lookBackUntilTs,
    )
    elements.push(...response.elements)
  }

  return elements
}
