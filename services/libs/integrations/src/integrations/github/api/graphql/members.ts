/* eslint-disable  @typescript-eslint/no-explicit-any */
import { graphql } from '@octokit/graphql'

import { GithubTokenRotator } from '../../tokenRotator'
import BaseQuery from './baseQuery'
import { Limiter } from './baseQuery'

/**
 * Get information from a member using the GitHub GraphQL API.
 * @param username GitHub username
 * @param token GitHub personal access token
 * @returns Information from member
 */
const getMember = async (
  username: string,
  token: string,
  tokenRotator?: GithubTokenRotator,
  limiter?: Limiter,
): Promise<any> => {
  let user: string | null
  try {
    const graphqlWithAuth = graphql.defaults({
      headers: {
        authorization: `token ${token}`,
      },
    })

    const process = async () => {
      user = (
        (await graphqlWithAuth(`{ 
            user(login: "${username}") ${BaseQuery.USER_SELECT}
          }
      `)) as any
      ).user
    }

    if (limiter) {
      await limiter.concurrentRequestLimiter.processWithLimit(limiter.integrationId, async () => {
        await process()
      })
    } else {
      await process()
    }
  } catch (err) {
    // It may be that the user was not found, if for example it is a bot
    // In that case we want to return null instead of throwing an error
    if (err.errors && err.errors[0].type === 'NOT_FOUND') {
      user = null
    } else if (
      (err.status === 403 &&
        err.message &&
        (err.message as string).toLowerCase().includes('secondary rate limit')) ||
      (err.errors && err.errors[0].type === 'RATE_LIMITED')
    ) {
      // this is rate limit, let's try token rotation
      if (tokenRotator && limiter) {
        user = await getMemberWithTokenRotation(username, tokenRotator, err, limiter)
      }
    } else {
      throw BaseQuery.processGraphQLError(err)
    }
  }
  return user
}

const getMemberWithTokenRotation = async (
  username: string,
  tokenRotator: GithubTokenRotator,
  err: any,
  limiter?: Limiter,
): Promise<any> => {
  let user: string | null
  const token = await tokenRotator.getToken(limiter.integrationId, err)
  try {
    const graphqlWithTokenRotation = graphql.defaults({
      headers: {
        authorization: `token ${token}`,
      },
    })

    const process = async () => {
      user = (
        (await graphqlWithTokenRotation(`{ 
            user(login: "${username}") ${BaseQuery.USER_SELECT}
          }
      `)) as any
      ).user
    }

    if (limiter) {
      await limiter.concurrentRequestLimiter.processWithLimit(limiter.integrationId, async () => {
        await process()
      })
    } else {
      await process()
    }

    await tokenRotator.updateRateLimitInfoFromApi(token)

    return user
  } catch (err) {
    if (err.errors && err.errors[0].type === 'NOT_FOUND') {
      return null
    }
    // we might have exhausted one token, but we let another streams to continue
    else if (
      err.headers &&
      err.headers['x-ratelimit-remaining'] &&
      err.headers['x-ratelimit-reset']
    ) {
      const remaining = parseInt(err.headers['x-ratelimit-remaining'])
      const reset = parseInt(err.headers['x-ratelimit-reset'])
      await tokenRotator.updateTokenInfo(token, remaining, reset)
    } else {
      await tokenRotator.updateRateLimitInfoFromApi(token)
    }
    throw BaseQuery.processGraphQLError(err)
  }
}

export default getMember
