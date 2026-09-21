import axios, { AxiosRequestConfig } from 'axios'

import { IProcessStreamContext } from '../../../types'
import { handleDiscordError } from './errorHandler'
import { retryWrapper } from './handleRateLimit'

export const getMessage = async (
  channelId: string,
  messageId: string,
  token: string,
  ctx: IProcessStreamContext,
) => {
  const config: AxiosRequestConfig = {
    method: 'get',
    url: `https://discord.com/api/v10/channels/${channelId}/messages/${messageId}`,
    headers: {
      Authorization: token,
    },
  }

  return await retryWrapper(3, async () => {
    try {
      const response = await axios(config)
      return response.data
    } catch (err) {
      if (
        err.response &&
        err.response.status === 404 &&
        err.response.data &&
        err.response.data.message === 'Unknown Channel' &&
        err.response.data.code === 10003
      ) {
        ctx.log.warn(
          { channelId, messageId },
          'Discord API returned Unknown Channel error when fetching message during webhook processing, skipping message.',
        )
        return null
      } else if (
        err.response &&
        err.response.status === 404 &&
        err.response.data &&
        err.response.data.message === 'Unknown Message' &&
        err.response.data.code === 10008
      ) {
        ctx.log.warn(
          { channelId, messageId },
          'Discord API returned Unknown Message error when fetching message during webhook processing, skipping message.',
        )
        return null
      } else if (err.response && err.response.status === 404) {
        ctx.log.warn(
          { channelId, messageId },
          'Discord API returned 404 error when fetching message during webhook processing, skipping message.',
        )
        return null
      } else {
        const newErr = handleDiscordError(err, config, { channelId, messageId }, ctx)
        if (newErr) {
          throw newErr
        }
      }
    }
  })
}
