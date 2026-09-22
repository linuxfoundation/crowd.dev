import axios, { AxiosRequestConfig } from 'axios'

import { timeout } from '@crowd/common'
import { PlatformType } from '@crowd/types'

import { IProcessStreamContext } from '../../../types'
import { getNangoToken } from '../../nango'
import { REDDIT_MAX_RETROSPECT_IN_HOURS, RedditGetPostsInput, RedditPostsResponse } from '../types'
import { handleRedditError } from './errorHandler'

/**
 * Get paginated posts from a subreddit
 * @param input RedditGetPostsInput. Made of a Nango ID to get the auth token, and a subreddit.
 * @param logger Logger instance for structured logging
 * @returns A reddit API response containing the posts in a subreddit.
 */
async function getPosts(
  input: RedditGetPostsInput,
  ctx: IProcessStreamContext,
): Promise<RedditPostsResponse> {
  let config: AxiosRequestConfig
  try {
    ctx.log.info({ message: 'Fetching posts from a sub-reddit', input })

    // Wait for 1.5s for rate limits.
    // eslint-disable-next-line no-promise-executor-return
    await timeout(1500)

    // Gett an access token from Nango
    const accessToken = await getNangoToken(input.nangoId, PlatformType.REDDIT, ctx)

    config = {
      method: 'get',
      url: `http://oauth.reddit.com/r/${input.subreddit}/new.json`,
      params: {
        limit: 100,
      },
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    }

    // If a pagination token was sent, add it to the request
    if (input.after) {
      config.params.after = input.after
    }

    const response: RedditPostsResponse = (await axios(config)).data

    // If ctx.onboarding is false, check the last post's date
    if (!ctx.onboarding) {
      const fiveHoursAgo = Date.now() / 1000 - 5 * 60 * 60 // in seconds
      const lastPost = response?.data?.children?.[response?.data?.children?.length - 1]

      if (lastPost && lastPost?.data?.created < fiveHoursAgo) {
        // If the last post is older than 5 hours, set 'after' to null so the next API call won't go deeper
        ctx.log.info(
          lastPost,
          `Last post is older than ${REDDIT_MAX_RETROSPECT_IN_HOURS} hours, stopping pagination`,
        )
        response.data.children[response.data.children.length - 1].data.name = null
      }
    }
    return response
  } catch (err) {
    ctx.log.error({ err, input }, 'Error while getting posts in subreddit')
    const newErr = handleRedditError(err, config, input, ctx)
    throw newErr
  }
}

export default getPosts
