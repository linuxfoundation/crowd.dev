import axios, { AxiosRequestConfig } from 'axios'

import { IProcessStreamContext } from '../../../types'
import { TwitterGetFollowersOutput, TwitterGetProfilesByUsernameInput } from '../types'
import { handleTwitterError } from './errorHandler'

/**
 * Get profiles by username
 * @param input Input parameters
 * @returns Profiles
 */
const getProfiles = async (
  input: TwitterGetProfilesByUsernameInput,
  ctx: IProcessStreamContext,
): Promise<TwitterGetFollowersOutput> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const config: AxiosRequestConfig<any> = {
    method: 'get',
    url: 'https://api.twitter.com/2/users/by',
    params: {
      usernames: input.usernames.join(','),
      'user.fields': 'name,description,location,public_metrics,url,verified,profile_image_url',
    },
    headers: {
      Authorization: `Bearer ${input.token}`,
    },
  }

  try {
    const response = await axios(config)
    const limit = parseInt(response.headers['x-rate-limit-remaining'], 10)
    const resetTs = parseInt(response.headers['x-rate-limit-reset'], 10) * 1000
    const currentTime = new Date().getTime()
    const timeUntilReset = Math.floor((resetTs - currentTime) / 1000)
    return {
      records: response.data.data,
      nextPage: response.data?.meta?.next_token || '',
      limit,
      timeUntilReset,
    }
  } catch (err) {
    const newErr = handleTwitterError(err, config, input, ctx.log)
    throw newErr
  }
}

export default getProfiles
