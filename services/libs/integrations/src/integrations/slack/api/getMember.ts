import axios, { AxiosRequestConfig } from 'axios'

import { timeout } from '@crowd/common'

import { IProcessStreamContext } from '../../../types'
import { SlackGetMemberInput, SlackGetMemberOutput } from '../types'
import { handleSlackError } from './errorHandler'

async function getMembers(
  input: SlackGetMemberInput,
  ctx: IProcessStreamContext,
): Promise<SlackGetMemberOutput> {
  await timeout(2000)

  const logger = ctx.log

  const config: AxiosRequestConfig = {
    method: 'get',
    url: `https://slack.com/api/users.info`,
    params: {
      user: input.userId,
    },
    headers: {
      Authorization: `Bearer ${input.token}`,
    },
  }

  try {
    const response = await axios(config)

    if (response.data.ok === true) {
      const member = response.data.user
      return {
        records: member,
        nextPage: '',
      }
    }

    if (response.data.error === 'user_not_found' || response.data.error === 'user_not_visible') {
      return {
        records: undefined,
        nextPage: '',
      }
    }

    throw new Error(`Slack API error ${response.data.error}!`)
  } catch (err) {
    const newErr = handleSlackError(err, config, input, logger)
    throw newErr
  }
}

export default getMembers
