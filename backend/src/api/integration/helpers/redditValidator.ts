import axios from 'axios'

import { REDDIT_CONFIG } from '@/conf'
import { Error400 } from '@crowd/common'
import { RedisCache, RedisClient } from '@crowd/redis'

import Permissions from '../../../security/permissions'
import track from '../../../segment/track'
import PermissionChecker from '../../../services/user/permissionChecker'

const getRedditToken = async (redis: RedisClient, logger: any) => {
  const cache = new RedisCache('reddit-global-access-token', redis, logger)
  const token = await cache.get('reddit-token')
  if (token) {
    return token
  }
  const result = await axios.post(
    'https://www.reddit.com/api/v1/access_token',
    'grant_type=client_credentials',
    {
      auth: {
        username: REDDIT_CONFIG.clientId,
        password: REDDIT_CONFIG.clientSecret,
      },
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    },
  )
  // cache for 30 minutes
  await cache.set('reddit-token', result.data.access_token, 30 * 60)
  return result.data.access_token
}

export default async (req, res) => {
  new PermissionChecker(req).validateHasAny([
    Permissions.values.integrationCreate,
    Permissions.values.integrationEdit,
  ])

  if (req.query.subreddit) {
    let token: string
    try {
      token = await getRedditToken(req.redis, req.log)
    } catch (e) {
      req.log.error(e)
      return req.responseHandler.error(req, res, new Error400(req.language))
    }
    try {
      const result = await axios.post(
        `https://oauth.reddit.com/api/search_reddit_names`,
        `query=${req.query.subreddit}&exact=true`,
        {
          headers: {
            ContentType: 'application/x-www-form-urlencoded',
            Authorization: `Bearer ${token}`,
          },
        },
      )
      if (
        result.status === 200 &&
        result.data.names &&
        result.data.names.includes(req.query.subreddit)
      ) {
        track(
          'Reddit: subreddit input',
          {
            subreddit: req.query.subreddit,
            valid: true,
          },
          { ...req },
        )
        return req.responseHandler.success(req, res, true)
      }
      // for other status codes we return error
      track(
        'Reddit: subreddit input',
        {
          subreddit: req.query.subreddit,
          valid: false,
        },
        { ...req },
      )
      return req.responseHandler.error(req, res, new Error400(req.language))
    } catch (e) {
      req.log.error('Error validating subreddit', e)
      track(
        'Reddit: subreddit input',
        {
          subreddit: req.query.subreddit,
          valid: false,
        },
        { ...req },
      )
      req.log.error(e)
      return req.responseHandler.error(req, res, new Error400(req.language))
    }
  }
  track(
    'Reddit: subreddit input',
    {
      subreddit: req.query.subreddit,
      valid: false,
    },
    { ...req },
  )
  req.log.error('Reddit: subreddit input is empty')
  return req.responseHandler.error(req, res, new Error400(req.language))
}
