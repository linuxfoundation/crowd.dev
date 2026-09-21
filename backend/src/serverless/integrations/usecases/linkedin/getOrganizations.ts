import axios, { AxiosRequestConfig } from 'axios'

import { Logger } from '@crowd/logging'
import { PlatformType } from '@crowd/types'

import { ILinkedInOrganization } from '../../types/linkedinTypes'
import getToken from '../nango/getToken'
import { handleLinkedinError } from './errorHandler'

export const getOrganizations = async (
  nangoId: string,
  logger: Logger,
): Promise<ILinkedInOrganization[]> => {
  const config: AxiosRequestConfig<any> = {
    method: 'get',
    url: `https://api.linkedin.com/v2/organizationAcls`,
    params: {
      q: 'roleAssignee',
      projection:
        '(elements*(*,roleAssignee~(localizedFirstName,localizedLastName),organization~(id,localizedName,vanityName,logoV2(original~:playableStreams))))',
    },
    headers: {
      'X-Restli-Protocol-Version': '2.0.0',
    },
  }
  try {
    logger.debug({ nangoId }, 'Fetching organizations from LinkedIn')

    // Get an access token from Nango
    const accessToken = await getToken(nangoId, PlatformType.LINKEDIN, logger)
    config.params.oauth2_access_token = accessToken

    const response = (await axios(config)).data

    return response.elements.map((e) => {
      let profilePictureUrl: string | undefined

      if (e['organization~'].logoV2?.['original~']?.elements?.length > 0) {
        const pictures = e['organization~'].logoV2['original~'].elements
        profilePictureUrl = pictures[pictures.length - 1].identifiers[0].identifier
      }

      return {
        id: e['organization~'].id,
        name: e['organization~'].localizedName,
        organizationUrn: e.organization,
        vanityName: e['organization~'].vanityName,
        profilePictureUrl,
      }
    })
  } catch (err) {
    const newErr = handleLinkedinError(err, config, { nangoId }, logger)
    throw newErr
  }
}
