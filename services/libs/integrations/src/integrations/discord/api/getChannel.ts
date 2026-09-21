import axios, { AxiosRequestConfig } from 'axios'

import { IProcessStreamContext } from '../../../types'
import { DiscordApiChannel } from '../types'
import { handleDiscordError } from './errorHandler'
import { retryWrapper } from './handleRateLimit'

export const getChannel = async (
  channelId: string,
  token: string,
  ctx: IProcessStreamContext,
): Promise<DiscordApiChannel> => {
  const config: AxiosRequestConfig = {
    method: 'get',
    url: `https://discord.com/api/v10/channels/${channelId}`,
    headers: {
      Authorization: token,
    },
  }

  return await retryWrapper(3, async () => {
    try {
      const response = await axios(config)
      return response.data
    } catch (err) {
      const newErr = handleDiscordError(err, config, { channelId }, ctx)
      if (newErr) {
        throw newErr
      }
    }
  })
}
