import axios, { AxiosRequestConfig } from 'axios'

import { IProcessStreamContext } from '../../../types'
import { DiscordApiMember } from '../types'
import { handleDiscordError } from './errorHandler'
import { retryWrapper } from './handleRateLimit'

export const getMember = async (
  guildId: string,
  userId: string,
  token: string,
  ctx: IProcessStreamContext,
): Promise<DiscordApiMember> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const config: AxiosRequestConfig<any> = {
    method: 'get',
    url: `https://discord.com/api/v10/guilds/${guildId}/members/${userId}`,
    headers: {
      Authorization: token,
    },
  }

  return await retryWrapper(3, async () => {
    try {
      const response = await axios(config)
      return response.data
    } catch (err) {
      const newErr = handleDiscordError(err, config, { guildId, userId }, ctx)
      if (newErr) {
        throw newErr
      }
    }
  })
}
