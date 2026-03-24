import axios, { AxiosRequestConfig } from 'axios'

import { PlatformType } from '@crowd/types'

import { IGenerateStreamsContext, IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'

import { handleLinkedinError } from './handleError'
import { ILinkedInOrganizationPost } from './types'

export const getPost = async (
  nangoId: string,
  postId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
): Promise<ILinkedInOrganizationPost> => {
  const config: AxiosRequestConfig<unknown> = {
    method: 'get',
    url: `https://api.linkedin.com/rest/posts/${postId}`,
    headers: {
      'X-Restli-Protocol-Version': '2.0.0',
      'LinkedIn-Version': '202601',
    },
  }

  try {
    ctx.log.debug({ nangoId, postId }, 'Fetching post!')
    // Get an access token from Nango
    const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)
    config.params.oauth2_access_token = accessToken

    const e = (await axios(config)).data

    return {
      urnId: e.id,
      lifecycleState: e.lifecycleState,
      visibility: e.visibility,
      authorUrn: e.author,
      body: e.commentary,
      originalUrnId: e.reshareContext?.parent,
      timestamp: e.createdAt,
    }
  } catch (err) {
    const newErr = handleLinkedinError(err, config, { nangoId, postId }, ctx.log)
    throw newErr
  }
}
