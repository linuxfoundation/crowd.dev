import axios, { AxiosRequestConfig } from 'axios'

import { IProcessStreamContext } from '../../../types'
import {
  TwitterGetPostsByHashtagInput,
  TwitterGetPostsOutput,
  TwitterParsedPosts,
  TwitterPlatformSettings,
} from '../types'
import { handleTwitterError } from './errorHandler'

/**
 * Get paginated posts by hashtag
 * @param input Input parameters
 * @returns Posts
 */
const getPostsByHashtag = async (
  input: TwitterGetPostsByHashtagInput,
  ctx: IProcessStreamContext,
): Promise<TwitterGetPostsOutput> => {
  const TWITTER_CONFIG = ctx.platformSettings as TwitterPlatformSettings
  const maxRetrospectInSeconds = parseInt(TWITTER_CONFIG.maxRetrospectInSeconds) || 7380

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const config: AxiosRequestConfig<any> = {
    method: 'get',
    url: 'https://api.twitter.com/2/tweets/search/recent',
    params: {
      max_results: input.perPage,
      'tweet.fields': 'id,text,created_at,entities,referenced_tweets,attachments',
      expansions: 'attachments.media_keys,author_id',
      'media.fields': 'duration_ms,height,media_key,preview_image_url,type,url,width,alt_text',
      'user.fields': 'name,description,location,public_metrics,url,verified,profile_image_url',
      query: `#${input.hashtag}`,
      ...(!ctx.onboarding && {
        start_time: new Date(Date.now() - maxRetrospectInSeconds * 1000).toISOString(),
      }),
    },
    headers: {
      Authorization: `Bearer ${input.token}`,
    },
  }

  if (input.page !== undefined && input.page !== '') {
    config.params.next_token = input.page
  }

  try {
    const response = await axios(config)
    const limit = parseInt(response.headers['x-rate-limit-remaining'], 10)
    const resetTs = parseInt(response.headers['x-rate-limit-reset'], 10) * 1000
    const currentTime = new Date().getTime()
    const timeUntilReset = Math.floor((resetTs - currentTime) / 1000)

    if (response.data?.meta?.result_count === 0) {
      return {
        records: [],
        limit,
        timeUntilReset,
        nextPage: '',
      }
    }

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
  } catch (err) {
    const newErr = handleTwitterError(err, config, input, ctx.log)
    throw newErr
  }
}

export default getPostsByHashtag
