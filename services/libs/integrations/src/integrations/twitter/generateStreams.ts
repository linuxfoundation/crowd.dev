import axios from 'axios'

import { GenerateStreamsHandler } from '../../types'
import { IGenerateStreamsContext } from '../../types'
import {
  TwitterHashtagStreamData,
  TwitterIntegrationsSettings,
  TwitterMentionsStreamData,
  TwitterPlatformSettings,
  TwitterReachStreamData,
  TwitterStreamType,
} from './types'

const CHECK_REACH_EVERY_IN_SECONDS = 24 * 60 * 60

// It should return true once a day
const reachNeedsUpdate = async (ctx: IGenerateStreamsContext): Promise<boolean> => {
  if (ctx.onboarding) return false

  const key = `twitter:reach:${ctx.integration.id}`

  const lastUpdate = await ctx.cache.get(key)

  if (!lastUpdate) {
    await ctx.cache.set(key, Date.now().toString(), CHECK_REACH_EVERY_IN_SECONDS)
    return true
  }

  return false
}

const refreshToken = async (ctx: IGenerateStreamsContext): Promise<void> => {
  const refreshToken = ctx.integration.refreshToken
  const TWITTER_CONFIG = ctx.platformSettings as TwitterPlatformSettings

  try {
    const encodedCredentials = Buffer.from(
      `${TWITTER_CONFIG.clientId}:${TWITTER_CONFIG.clientSecret}`,
    ).toString('base64')
    const response = await axios.post('https://api.twitter.com/2/oauth2/token', null, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${encodedCredentials}`,
      },
      params: {
        refresh_token: refreshToken,
        grant_type: 'refresh_token',
      },
    })

    const { access_token, refresh_token } = response.data

    await ctx.updateIntegrationToken(access_token)
    await ctx.updateIntegrationRefreshToken(refresh_token)
  } catch (e) {
    await ctx.abortRunWithError(
      'Error refreshing Twitter token, aborting run',
      {
        refreshToken,
        TWITTER_CONFIG,
      },
      e,
    )
  }
}

const handler: GenerateStreamsHandler = async (ctx) => {
  await refreshToken(ctx)

  const settings = ctx.integration.settings as TwitterIntegrationsSettings
  const hashtags = settings.hashtags

  // first let's kick off mentions stream
  await ctx.publishStream<TwitterMentionsStreamData>(TwitterStreamType.MENTIONS, {
    page: '',
  })

  for (const hashtag of hashtags) {
    await ctx.publishStream<TwitterHashtagStreamData>(`${TwitterStreamType.HASHTAG}:${hashtag}`, {
      hashtag,
      page: '',
    })
  }

  // check if we need to update reach
  if (await reachNeedsUpdate(ctx)) {
    await ctx.publishStream<TwitterReachStreamData>(TwitterStreamType.REACH, {})
  }
}

export default handler
