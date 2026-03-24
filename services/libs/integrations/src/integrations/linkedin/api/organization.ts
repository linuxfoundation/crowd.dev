import axios, { AxiosRequestConfig } from 'axios'

import { PlatformType } from '@crowd/types'

import { IGenerateStreamsContext, IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'

import { handleLinkedinError } from './handleError'
import { ILinkedInOrganization } from './types'

export const getOrganization = async (
  nangoId: string,
  organizationId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
): Promise<ILinkedInOrganization> => {
  const config: AxiosRequestConfig<unknown> = {
    method: 'get',
    url: `https://api.linkedin.com/rest/organizationsLookup`,
    params: {
      ids: `List(${organizationId})`,
      projection:
        '(results*(id,vanityName,localizedName,name,locations,localizedWebsite,logoV2(original~:playableStreams)))',
    },
    headers: {
      'X-Restli-Protocol-Version': '2.0.0',
      'LinkedIn-Version': '202601',
    },
  }

  try {
    ctx.log.debug({ nangoId, organizationId }, 'Fetching organization from LinkedIn')

    // Get an access token from Nango
    const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)
    config.params.oauth2_access_token = accessToken

    const result = await axios(config)

    const response = result.data.results[organizationId]

    if (!response) {
      return {
        id: parseInt(organizationId, 10),
        name: 'private',
        organizationUrn: `urn:li:organization:${organizationId}`,
        vanityName: 'private',
      }
    }

    let profilePictureUrl: string | undefined

    if (response.logoV2?.['original~']?.elements?.length > 0) {
      const pictures = response.logoV2['original~'].elements
      profilePictureUrl = pictures[pictures.length - 1].identifiers[0].identifier
    }

    return {
      id: parseInt(organizationId, 10),
      name: response.localizedName,
      organizationUrn: `urn:li:organization:${organizationId}`,
      vanityName: response.vanityName,
      profilePictureUrl,
    }
  } catch (err) {
    const newErr = handleLinkedinError(err, config, { nangoId, organizationId }, ctx.log)
    throw newErr
  }
}

export const getOrganizations = async (
  nangoId: string,
  ctx: IProcessStreamContext | IGenerateStreamsContext,
): Promise<ILinkedInOrganization[]> => {
  const config: AxiosRequestConfig<unknown> = {
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
    ctx.log.debug({ nangoId }, 'Fetching organizations from LinkedIn')

    // Get an access token from Nango
    const accessToken = await getNangoToken(nangoId, PlatformType.LINKEDIN, ctx)
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
    const newErr = handleLinkedinError(err, config, { nangoId }, ctx.log)
    throw newErr
  }
}
