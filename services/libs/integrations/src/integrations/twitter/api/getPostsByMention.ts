import axios, { AxiosRequestConfig } from 'axios'

import { IProcessStreamContext } from '../../../types'
import {
  TwitterGetPostsByMentionInput,
  TwitterGetPostsOutput,
  TwitterParsedPosts,
  TwitterPlatformSettings,
} from '../types'
import { handleTwitterError } from './errorHandler'

/**
 * Get paginated posts by mention
 * @param input Input parameters
 * @returns Posts
 */
const getPostsByMention = async (
  input: TwitterGetPostsByMentionInput,
  ctx: IProcessStreamContext,
): Promise<TwitterGetPostsOutput> => {
  const TWITTER_CONFIG = ctx.platformSettings as TwitterPlatformSettings
  const maxRetrospectInSeconds = parseInt(TWITTER_CONFIG.maxRetrospectInSeconds) || 7380

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const config: AxiosRequestConfig<any> = {
    method: 'get',
    url: `https://api.twitter.com/2/users/${input.profileId}/mentions`,
    params: {
      max_results: input.perPage,
      'tweet.fields': 'id,text,created_at,attachments,referenced_tweets,entities',
      'media.fields': 'duration_ms,height,media_key,preview_image_url,type,url,width,alt_text',
      'user.fields': 'name,description,location,public_metrics,url,verified,profile_image_url',
      expansions: 'attachments.media_keys,author_id',
      ...(!ctx.onboarding && {
        start_time: new Date(Date.now() - maxRetrospectInSeconds * 1000).toISOString(),
      }),
    },
    headers: {
      Authorization: `Bearer ${input.token}`,
    },
  }

  if (input.page !== undefined && input.page !== '') {
    config.params.pagination_token = input.page
  }

  try {
    const response = await axios(config)

    let limit: number
    let timeUntilReset: number
    if (response.headers['x-rate-limit-remaining'] && response.headers['x-rate-limit-reset']) {
      limit = parseInt(response.headers['x-rate-limit-remaining'], 10)
      const resetTs = parseInt(response.headers['x-rate-limit-reset'], 10) * 1000
      const currentTime = new Date().getTime()
      timeUntilReset = Math.floor((resetTs - currentTime) / 1000)
    } else {
      limit = 0
      timeUntilReset = 0
    }

    if (
      response.data.meta &&
      response.data.meta.result_count &&
      response.data.meta.result_count > 0
    ) {
      const posts = response.data.data
      const media = response.data.includes.media
      const users = response.data.includes.users

      const postsOut: TwitterParsedPosts = []

      for (const post of posts) {
        if (post.attachments?.media_keys) {
          const computedMedia = post.attachments.media_keys.map((key) =>
            media.find((m) => m.media_key === key),
          )
          post.attachments = computedMedia
        }
        const member = users.find((u) => u.id === post.author_id)
        post.member = member
        postsOut.push(post)
      }

      return {
        records: postsOut,
        nextPage: response.data?.meta?.next_token || '',
        limit,
        timeUntilReset,
      }
    }
    return {
      records: [],
      nextPage: '',
      limit,
      timeUntilReset,
    }
  } catch (err) {
    const newErr = handleTwitterError(err, config, input, ctx.log)
    throw newErr
  }
}

export default getPostsByMention
