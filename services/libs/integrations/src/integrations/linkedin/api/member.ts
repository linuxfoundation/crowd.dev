import axios, { AxiosRequestConfig } from 'axios'

import { PlatformType } from '@crowd/types'

import { IGenerateStreamsContext, IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'
import { handleLinkedinError } from './handleError'
import { ILinkedInMember } from './types'

export const getMember = async (
  nangoId: string,
  memberId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
): Promise<ILinkedInMember> => {
  const config: AxiosRequestConfig<unknown> = {
    method: 'get',
    url: `https://api.linkedin.com/v2/people/(id:${memberId})`,
    params: {
      projection:
        '(id,firstName,localizedFirstName,localizedLastName,vanityName,profilePicture(displayImage~:playableStreams))',
    },
    headers: {
      'X-Restli-Protocol-Version': '2.0.0',
    },
  }

  try {
    ctx.log.debug({ nangoId, memberId }, 'Fetching member data!')
    // Get an access token from Nango
    const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)
    config.params.oauth2_access_token = accessToken

    const response = (await axios(config)).data

    if (response.id === undefined || response.id === 'private') {
      return {
        id: 'private',
        vanityName: 'private',
        firstName: 'private',
        lastName: 'private',
        country: 'private',
      }
    }

    let profilePictureUrl: string | undefined
    if (response.profilePicture?.['displayImage~']?.elements?.length > 0) {
      const pictures = response.profilePicture['displayImage~'].elements
      profilePictureUrl = pictures[pictures.length - 1].identifiers[0].identifier
    }

    return {
      id: response.id,
      vanityName: response.vanityName,
      firstName: response.localizedFirstName,
      lastName: response.localizedLastName,
      country: response.firstName.preferredLocale.country,
      profilePictureUrl,
    }
  } catch (err) {
    if (err.response?.status === 403 || err.response?.status === 404) {
      // we can't access this profile because of visibility settings
      return {
        id: 'private',
        vanityName: 'private',
        firstName: 'private',
        lastName: 'private',
        country: 'private',
      }
    }
    const newErr = handleLinkedinError(err, config, { nangoId, memberId }, ctx.log)
    throw newErr
  }
}
