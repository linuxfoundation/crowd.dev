import axios from 'axios'

import { IProcessStreamContext } from '../../../types'
import { DiscordApiMember, DiscordGetMembersInput, DiscordGetMembersOutput } from '../types'
import { handleDiscordError } from './errorHandler'
import { retryWrapper } from './handleRateLimit'

async function getMembers(
  input: DiscordGetMembersInput,
  ctx: IProcessStreamContext,
): Promise<DiscordGetMembersOutput> {
  let url = `https://discord.com/api/v10/guilds/${input.guildId}/members?limit=${input.perPage}`
  if (input.page !== undefined && input.page !== '') {
    url += `&after=${input.page}`
  }

  const config = {
    method: 'get',
    url,
    headers: {
      Authorization: input.token,
    },
  }

  return await retryWrapper(3, async () => {
    try {
      const response = await axios(config)
      const records: DiscordApiMember[] = response.data
      const limit = parseInt(response.headers['x-ratelimit-remaining'], 10)
      const timeUntilReset = parseInt(response.headers['x-ratelimit-reset-after'], 10)
      const nextPage = records.length > 0 ? (records[records.length - 1].user.id as string) : ''
      return {
        records,
        nextPage,
        limit,
        timeUntilReset,
      }
    } catch (err) {
      const newErr = handleDiscordError(err, config, { input }, ctx)
      if (newErr) {
        throw newErr
      }
    }
  })
}

export default getMembers
