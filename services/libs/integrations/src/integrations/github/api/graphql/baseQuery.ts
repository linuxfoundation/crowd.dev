/* eslint-disable  @typescript-eslint/no-explicit-any */
import { graphql } from '@octokit/graphql'
import { GraphQlQueryResponseData } from '@octokit/graphql/dist-types/types'

import { getServiceChildLogger } from '@crowd/logging'
import { GraphQlQueryResponse } from '@crowd/types'
import { IConcurrentRequestLimiter, RateLimitError } from '@crowd/types'

import { GithubTokenRotator } from '../../tokenRotator'

const logger = getServiceChildLogger('integrations:github:api:graphql:baseQuery')

export interface Limiter {
  integrationId: string
  concurrentRequestLimiter: IConcurrentRequestLimiter
}

class BaseQuery {
  static BASE_URL = 'https://api.github.com/graphql'

  static USER_SELECT = `{
        login
        name
        avatarUrl
        id
        isHireable
        twitterUsername
        url
        websiteUrl
        email
        bio
        company
        companyHTML
        location
        followers {
          totalCount
        }
    }`

  static USER_SELECT_WITH_TYPENAME = `{
        __typename
        login
        name
        avatarUrl
        id
        isHireable
        twitterUsername
        url
        websiteUrl
        email
        bio
        company
        companyHTML
        location
        followers {
          totalCount
        }
    }`

  static ORGANIZATION_SELECT = `{
    login
    email
    url
    location
    name
    twitterUsername
    websiteUrl
    description
    avatarUrl
  }`

  static PAGE_SELECT = `{
        hasPreviousPage
        startCursor
    }`

  static BOT_SELECT = `{
    login
    avatarUrl
    id
    url
  }`

  graphQL

  query

  githubToken

  additionalHeaders

  perPage

  eventType

  constructor(githubToken: string, query: string, eventType: string, perPage: number) {
    this.githubToken = githubToken
    this.query = query
    this.perPage = perPage
    this.eventType = eventType
    this.graphQL = graphql.defaults({
      headers: {
        authorization: `token ${this.githubToken}`,
      },
    })
  }

  /**
   * Substitutes a variable like string $var with given variable
   * in a string. Useful when reusing the same string template
   * for multiple graphql paging requests.
   * $var in the string is substituted with obj[var]
   * @param str string to make the substitution
   * @param obj object containing variable to interpolate
   * @returns interpolated string
   */
  static interpolate(str: string, obj: any): string {
    return str.replace(/\${([^}]+)}/g, (_, prop) => obj[prop])
  }

  /**
   * Gets a single page result given a cursor.
   * Single page before the given cursor will be fetched.
   * @param beforeCursor Cursor to paginate records before it
   * @returns parsed graphQl result
   */
  async getSinglePage(
    beforeCursor: string,
    limiter?: Limiter,
    tokenRotator?: GithubTokenRotator,
  ): Promise<GraphQlQueryResponse> {
    const paginatedQuery = BaseQuery.interpolate(this.query, {
      beforeCursor: BaseQuery.getPagination(beforeCursor),
    })

    const process = async () => {
      const result = await this.graphQL(paginatedQuery)
      return this.getEventData(result)
    }

    try {
      if (limiter) {
        return await limiter.concurrentRequestLimiter.processWithLimit(
          limiter.integrationId,
          async () => {
            return await process()
          },
        )
      } else {
        return await process()
      }
    } catch (err) {
      logger.error('Error in getSinglePage')
      if (
        (err.status === 403 &&
          err.message &&
          (err.message as string).toLowerCase().includes('secondary rate limit')) ||
        (err.errors && err.errors[0].type === 'RATE_LIMITED') ||
        (err.message &&
          (err.message as string)
            .toLowerCase()
            .includes('although you appear to have the correct authorization credentials'))
      ) {
        logger.error('Error in getSinglePage: rate limit error. Trying token rotation')
        // this is rate limit, let's try token rotation
        if (tokenRotator && limiter) {
          return await this.getSinglePageWithTokenRotation(beforeCursor, tokenRotator, err, limiter)
        } else {
          throw BaseQuery.processGraphQLError(err)
        }
      } else {
        logger.error('Error in getSinglePage: other error')
        throw BaseQuery.processGraphQLError(err)
      }
    }
  }

  private async getSinglePageWithTokenRotation(
    beforeCursor: string,
    tokenRotator: GithubTokenRotator,
    err: any,
    limiter?: Limiter,
  ): Promise<GraphQlQueryResponse> {
    logger.info('getSinglePageWithTokenRotation')

    const paginatedQuery = BaseQuery.interpolate(this.query, {
      beforeCursor: BaseQuery.getPagination(beforeCursor),
    })

    const token = await tokenRotator.getToken(limiter.integrationId, err)

    const process = async () => {
      const graphqlWithTokenRotation = graphql.defaults({
        headers: {
          authorization: `token ${token}`,
        },
      })
      const result = await graphqlWithTokenRotation(paginatedQuery)
      await tokenRotator.updateRateLimitInfoFromApi(token)
      return this.getEventData(result)
    }

    try {
      if (limiter) {
        return await limiter.concurrentRequestLimiter.processWithLimit(
          limiter.integrationId,
          async () => {
            return await process()
          },
        )
      } else {
        return await process()
      }
    } catch (err) {
      logger.error('Error in getSinglePageWithTokenRotation')
      // we might have exhausted one token, but we let another streams to continue
      if (err.headers && err.headers['x-ratelimit-remaining'] && err.headers['x-ratelimit-reset']) {
        const remaining = parseInt(err.headers['x-ratelimit-remaining'])
        const reset = parseInt(err.headers['x-ratelimit-reset'])
        await tokenRotator.updateTokenInfo(token, remaining, reset)
      } else {
        await tokenRotator.updateRateLimitInfoFromApi(token)
      }
      throw BaseQuery.processGraphQLError(err)
    }
  }

  /**
   * Parses graphql result into an object.
   * Object contains information about paging, and fetched data.
   * @param result from graphql query
   * @returns parsed result into paging and data values.
   */
  getEventData(result: GraphQlQueryResponseData): GraphQlQueryResponse {
    return {
      hasPreviousPage: result.repository[this.eventType].pageInfo?.hasPreviousPage,
      startCursor: result.repository[this.eventType].pageInfo?.startCursor,
      data: [{}],
    }
  }

  /**
   * Returns pagination string given cursor.
   * @param beforeCursor cursor to use for the pagination
   * @returns pagination string that can be injected into a graphql query.
   */
  static getPagination(beforeCursor: string): string {
    if (beforeCursor) {
      return `before: "${beforeCursor}"`
    }
    return ''
  }

  static processGraphQLError(err: any): any {
    if (
      (err.status === 403 &&
        err.message &&
        (err.message as string).toLowerCase().includes('secondary rate limit')) ||
      (err.errors && err.errors[0].type === 'RATE_LIMITED')
    ) {
      if (err.headers && err.headers['x-ratelimit-reset']) {
        const query =
          err.request && err.request.query ? err.request.query : 'Unknown GraphQL query!'

        const epochReset = parseInt(err.headers['x-ratelimit-reset'], 10)
        const resetDate = new Date(epochReset * 1000) // JavaScript Date constructor takes milliseconds
        const diffInSeconds = Math.floor((resetDate.getTime() - Date.now()) / 1000)

        return new RateLimitError(diffInSeconds + 5, query, err)
      }
    }

    return err
  }
}

export default BaseQuery

export const process = async () => {
  try {
    return process()
  } catch {
    // some logic here
  }
}
