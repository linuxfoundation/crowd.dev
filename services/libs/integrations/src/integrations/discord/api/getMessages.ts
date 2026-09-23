import axios from 'axios'

import { IProcessStreamContext } from '../../../types'
import { DiscordApiMessage, DiscordGetMessagesInput, DiscordParsedReponse } from '../types'
import { handleDiscordError } from './errorHandler'
import { retryWrapper } from './handleRateLimit'

async function getMessages(
  input: DiscordGetMessagesInput,
  ctx: IProcessStreamContext,
  showErrors = true,
): Promise<DiscordParsedReponse> {
  let url = `https://discord.com/api/v10/channels/${input.channelId}/messages?limit=${input.perPage}`
  if (input.page !== undefined && input.page !== '') {
    url += `&before=${input.page}`
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
      const records: DiscordApiMessage[] = response.data

      const limit = parseInt(response.headers['x-ratelimit-remaining'], 10)
      const timeUntilReset = parseInt(response.headers['x-ratelimit-reset-after'], 10)
      const nextPage = records.length > 0 ? records[records.length - 1].id : ''
      return {
        records,
        nextPage,
        limit,
        timeUntilReset,
      }
    } catch (err) {
      if (!showErrors) {
        return {
          records: [],
          nextPage: '',
          limit: 0,
          timeUntilReset: 0,
        }
      }
      const newErr = handleDiscordError(err, config, { input }, ctx)
      if (newErr) {
        throw newErr
      }
    }
  })
}

export default getMessages
