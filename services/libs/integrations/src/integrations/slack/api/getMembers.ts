import axios, { AxiosRequestConfig } from 'axios'

import { timeout } from '@crowd/common'

import { IProcessStreamContext } from '../../../types'
import { SlackGetMembersInput, SlackGetMembersOutput, SlackMembers } from '../types'
import { handleSlackError } from './errorHandler'

async function getMembers(
  input: SlackGetMembersInput,
  ctx: IProcessStreamContext,
): Promise<SlackGetMembersOutput> {
  await timeout(2000)

  const logger = ctx.log

  const config: AxiosRequestConfig = {
    method: 'get',
    url: 'https://slack.com/api/users.list',
    params: {},
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

  try {
    const response = await axios(config)
    const records: SlackMembers = response.data.members
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

export default getMembers
