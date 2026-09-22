import axios from 'axios'

import { RedisCache } from '@crowd/redis'

import { API_CONFIG, TWITTER_CONFIG } from '../../../conf'
import SegmentRepository from '../../../database/repositories/segmentRepository'
import Permissions from '../../../security/permissions'
import IntegrationService from '../../../services/integrationService'
import PermissionChecker from '../../../services/user/permissionChecker'

const errorURL = `${API_CONFIG.frontendUrl}/integrations?twitter-error=true`

const decodeBase64Url = (data) => {
  data = data.replaceAll('-', '+').replaceAll('_', '/')
  while (data.length % 4) {
    data += '='
  }
  return atob(data)
}

export default async (req, res) => {
  // Checking we have permision to edit the integration
  new PermissionChecker(req).validateHas(Permissions.values.integrationEdit)

  const cache = new RedisCache('twitterPKCE', req.redis, req.log)

  const userId = req.currentUser.id
  const decodedState = decodeBase64Url(req.query.state)
  const externalState = JSON.parse(decodedState)

  const { handle } = externalState

  const existingValue = await cache.get(userId)
  if (!existingValue) {
    res.redirect(errorURL)
  }

  const stateObj = JSON.parse(existingValue)

  await cache.delete(userId)
  if (stateObj.handle !== handle) {
    res.redirect(errorURL)
  }

  const callbackUrl = stateObj.callbackUrl
  const redirectUrl = stateObj.redirectUrl
  const codeVerifier = stateObj.codeVerifier
  const segmentIds = stateObj.segmentIds
  const oauthVerifier = req.query.code
  const hashtags = stateObj.hashtags

  // attach segments to request
  const segmentRepository = new SegmentRepository(req)
  req.currentSegments = await segmentRepository.findInIds(segmentIds)

  try {
    const response = await axios.post(
      'https://api.twitter.com/2/oauth2/token',
      {},
      {
        params: {
          client_id: TWITTER_CONFIG.clientId,
          code: oauthVerifier,
          grant_type: 'authorization_code',
          redirect_uri: callbackUrl,
          code_verifier: codeVerifier,
        },
        auth: {
          username: TWITTER_CONFIG.clientId,
          password: TWITTER_CONFIG.clientSecret,
        },
      },
    )

    // with the token let's get user info
    const userResponse = await axios.get('https://api.twitter.com/2/users/me', {
      headers: {
        Authorization: `Bearer ${response.data.access_token}`,
      },
    })

    const twitterUserId = userResponse.data.data.id

    const integrationData = {
      profileId: twitterUserId,
      token: response.data.access_token,
      refreshToken: response.data.refresh_token,
      hashtags,
    }
    await new IntegrationService(req).twitterCallback(integrationData)

    res.redirect(redirectUrl)
  } catch {
    res.redirect(errorURL)
  }
}
