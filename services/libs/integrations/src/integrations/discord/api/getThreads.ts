import axios from 'axios'

import { IProcessStreamContext } from '../../../types'
import { DiscordApiChannel, DiscordGetChannelsInput } from '../types'
import { handleDiscordError } from './errorHandler'
import { retryWrapper } from './handleRateLimit'

async function getThreads(
  input: DiscordGetChannelsInput,
  ctx: IProcessStreamContext,
): Promise<DiscordApiChannel[]> {
  const config = {
    method: 'get',
    url: `https://discord.com/api/v10/guilds/${input.guildId}/threads/active?`,
    headers: {
      Authorization: input.token,
    },
  }

  return await retryWrapper(3, async () => {
    try {
      const response = await axios(config)
      return response.data.threads
    } catch (err) {
      const newErr = handleDiscordError(err, config, { input }, ctx)
      if (newErr) {
        throw newErr
      }
    }
  })
}

export default getThreads
