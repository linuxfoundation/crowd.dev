import axios, { AxiosRequestConfig } from 'axios'

import { Logger } from '@crowd/logging'
import { RateLimitError } from '@crowd/types'

import type { DiscourseConnectionParams } from '../../types/discourseTypes'
import { DiscoursePostsByIdsInput, DiscoursePostsByIdsResponse } from '../../types/discourseTypes'

const serializeObjectToQueryString = (params: object) =>
  Object.entries(params)
    .map(([key, value]) => {
      if (Array.isArray(value)) {
        return value
          .map((val) => `${encodeURIComponent(key)}[]=${encodeURIComponent(val)}`)
          .join('&')
      }
      return `${encodeURIComponent(key)}=${encodeURIComponent(value)}`
    })
    .join('&')

// this methods returns ids of posts in a topic
// then we need to parse each topic individually (can be batched)
export const getDiscoursePostsByIds = async (
  params: DiscourseConnectionParams,
  input: DiscoursePostsByIdsInput,
  logger: Logger,
): Promise<DiscoursePostsByIdsResponse> => {
  logger.info({
    message: 'Fetching posts by ids from Discourse',
    params,
    input,
  })

  const queryParameters = {
    post_ids: input.post_ids,
  }

  const queryString = serializeObjectToQueryString(queryParameters)

  const config: AxiosRequestConfig<any> = {
    method: 'get',
    url: `${params.forumHostname}/t/${input.topic_id}/posts.json?${queryString}`,
    headers: {
      'Api-Key': params.apiKey,
      'Api-Username': params.apiUsername,
    },
  }

  try {
    const response = await axios(config)
    return response.data
  } catch (err) {
    if (err.response && err.response.status === 429) {
      // wait 5 mins
      throw new RateLimitError(5 * 60, 'discourse/getpostsbyids')
    }
    logger.error({ err, params, input }, 'Error while getting posts by ids from Discourse ')
    throw err
  }
}
