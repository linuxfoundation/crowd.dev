/* eslint-disable  @typescript-eslint/no-explicit-any */
import { graphql } from '@octokit/graphql'

import { getServiceChildLogger } from '@crowd/logging'

import { GithubTokenRotator } from '../../tokenRotator'
import BaseQuery from './baseQuery'
import { Limiter } from './baseQuery'

const logger = getServiceChildLogger('github.getOrganization')

/**
 * Get information from a organization using the GitHub GraphQL API.
 * @param name Name of the organization in GitHub
 * @param token GitHub personal access token
 * @returns Information from organization
 */
const getOrganization = async (
  name: string,
  token: string,
  tokenRotator?: GithubTokenRotator,
  limiter?: Limiter,
): Promise<any> => {
  let organization: string | null
  try {
    const graphqlWithAuth = graphql.defaults({
      headers: {
        authorization: `token ${token}`,
      },
    })

    const sanitizedName = name.replaceAll('\\', '').replaceAll('"', '')

    const organizationsQuery = `{
      organization(login: "${sanitizedName}") {
        ... on Organization ${BaseQuery.ORGANIZATION_SELECT}
      }
      rateLimit {
          limit
          cost
          remaining
          resetAt
      }
      }`

    const process = async () => {
      organization = (await graphqlWithAuth(organizationsQuery)) as any

      organization =
        (organization as any).organization && (organization as any).organization.login
          ? (organization as any).organization
          : null
    }

    if (limiter) {
      await limiter.concurrentRequestLimiter.processWithLimit(limiter.integrationId, async () => {
        await process()
      })
    } else {
      await process()
    }
  } catch (err) {
    logger.error(err, { name }, 'Error getting organization!')
    // It may be that the organization was not found, if for example it is a bot
    // In that case we want to return null instead of throwing an error
    if (err.errors && err.errors[0].type === 'NOT_FOUND') {
      organization = null
    } else if (
      (err.status === 403 &&
        err.message &&
        (err.message as string).toLowerCase().includes('secondary rate limit')) ||
      (err.errors && err.errors[0].type === 'RATE_LIMITED') ||
      err?.message?.toLowerCase().includes('rate limit exceeded')
    ) {
      logger.info('Rate limit error detected in getOrganization')
      // this is rate limit, let's try token rotation
      if (tokenRotator && limiter) {
        logger.info('Trying token rotation in getOrganization')
        organization = await getOrganizationWithTokenRotation(name, tokenRotator, err, limiter)
      }
    } else if (
      err?.message
        ?.toLowerCase()
        .includes('although you appear to have the correct authorization credentials')
    ) {
      logger.info('Token is not valid in getOrganization')
      organization = null
    } else {
      throw BaseQuery.processGraphQLError(err)
    }
  }
  return organization
}

const getOrganizationWithTokenRotation = async (
  name: string,
  tokenRotator: GithubTokenRotator,
  err: any,
  limiter?: Limiter,
): Promise<any> => {
  let organization: string | null
  let token: string
  try {
    token = await tokenRotator.getToken(limiter.integrationId, err)
    logger.info('Got token from token rotator in getOrganizationWithTokenRotation')
    const graphqlWithTokenRotation = graphql.defaults({
      headers: {
        authorization: `token ${token}`,
      },
    })

    const sanitizedName = name.replaceAll('\\', '').replaceAll('"', '')

    const organizationsQuery = `{
      organization(login: "${sanitizedName}") {
        ... on Organization ${BaseQuery.ORGANIZATION_SELECT}
      }
      rateLimit {
          limit
          cost
          remaining
          resetAt
      }
      }`

    const process = async () => {
      organization = (await graphqlWithTokenRotation(organizationsQuery)) as any

      organization =
        (organization as any).organization && (organization as any).organization.login
          ? (organization as any).organization
          : null
    }

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
  } catch (err1) {
    if (err1.errors && err1.errors[0].type === 'NOT_FOUND') {
      logger.info('Organization not found in getOrganizationWithTokenRotation')
      return null
    } else if (
      err1.headers &&
      err1.headers['x-ratelimit-remaining'] &&
      err1.headers['x-ratelimit-reset']
    ) {
      logger.info('Rate limit error detected in getOrganizationWithTokenRotation')
      const remaining = parseInt(err1.headers['x-ratelimit-remaining'])
      const reset = parseInt(err1.headers['x-ratelimit-reset'])
      await tokenRotator.updateTokenInfo(token, remaining, reset)
    } else if (
      err?.message
        ?.toLowerCase()
        .includes('although you appear to have the correct authorization credentials')
    ) {
      logger.info('Token is not valid in getOrganization')
      organization = null
    } else {
      logger.info('Other error detected in getOrganizationWithTokenRotation')
      await tokenRotator.updateRateLimitInfoFromApi(token)
    }
    throw BaseQuery.processGraphQLError(err1)
  }
}

export default getOrganization
