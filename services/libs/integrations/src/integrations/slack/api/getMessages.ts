import axios, { AxiosRequestConfig } from 'axios'

import { timeout } from '@crowd/common'

import { IProcessStreamContext } from '../../../types'
import {
  ISlackPlatformSettings,
  SlackGetMessagesInput,
  SlackMessages,
  SlackParsedResponse,
} from '../types'
import { handleSlackError } from './errorHandler'

async function getMessages(
  input: SlackGetMessagesInput,
  ctx: IProcessStreamContext,
): Promise<SlackParsedResponse> {
  await timeout(2000)

  const logger = ctx.log

  const platformSettings = ctx.platformSettings as ISlackPlatformSettings
  const maxRetrospectInSeconds = platformSettings.maxRetrospectInSeconds

  const config: AxiosRequestConfig = {
    method: 'get',
    url: `https://slack.com/api/conversations.history`,
    params: {
      channel: input.channelId,
    },
    headers: {
      Authorization: `Bearer ${input.token}`,
    },
  }

  if (input.page !== undefined && input.page !== '') {
    config.params.cursor = input.page
  }

  if (input.perPage !== undefined && input.perPage > 0) {
    config.params.limit = input.perPage
  }

  if (!ctx.onboarding && !input.new) {
    // we don't want to get messages older than maxRetrospectInSeconds during incremental sync
    // but if it's a completely new channel, we want to get all messages
    config.params.oldest = new Date(Date.now() - maxRetrospectInSeconds * 1000).getTime() / 1000
  }

  try {
    const response = await axios(config)
    const records: SlackMessages = response.data.messages

    const nextPage = response.data.response_metadata?.next_cursor || ''
    return {
      records,
      nextPage,
    }
  } catch (err) {
    const newErr = handleSlackError(err, config, input, logger)
    throw newErr
  }
}

export default getMessages
