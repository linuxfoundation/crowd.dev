import {
  getGithubIdentitiesWithoutSourceId as getGithubIdentities,
  updateIdentitySourceId as updateSourceId,
} from '@crowd/data-access-layer/src/old/apps/members_enrichment_worker'
import { GithubAPIResource, GithubTokenRotator } from '@crowd/integrations'
import { RedisCache } from '@crowd/redis'
import { IMemberIdentity } from '@crowd/types'

import { svc } from '../../service'
import { IGithubUser } from '../../sources/lfid/types'

export async function getGithubIdentitiesWithoutSourceId(
  limit: number,
  afterId: string,
  afterUsername: string,
): Promise<IMemberIdentity[]> {
  let rows: IMemberIdentity[] = []

  try {
    const db = svc.postgres.reader
    rows = await getGithubIdentities(db, limit, afterId, afterUsername)
  } catch (err) {
    throw new Error(err)
  }

  return rows
}

export async function updateIdentitySourceId(
  identity: IMemberIdentity,
  sourceId: string,
): Promise<void> {
  try {
    const db = svc.postgres.writer
    await updateSourceId(db, identity, sourceId)
  } catch (err) {
    throw new Error(err)
  }
}

/**
 * Returns true if there are credits in some tokens for use,
 * false otherwise.
 */
export async function checkTokens(): Promise<boolean> {
  const cache = new RedisCache(`lfx-auth0`, svc.redis, svc.log)
  const tokenRotator = new GithubTokenRotator(
    cache,
    process.env['CROWD_GITHUB_PERSONAL_ACCESS_TOKENS'].split(','),
  )

  const tokens = await tokenRotator.getAllTokens()

  for (const token of tokens) {
    try {
      await tokenRotator.updateRateLimitInfoFromApi(token, GithubAPIResource.CORE)
    } catch {
      // something is wrong with the token, remove it from the list
      tokenRotator.removeToken(token)
    }
  }

  try {
    const token = await tokenRotator.getToken()
    if (token) {
      return true
    }
  } catch {
    return false
  }
}

export async function findGithubSourceId(username: string): Promise<string | null> {
  try {
    const cache = new RedisCache(`lfx-auth0`, svc.redis, svc.log)
    const tokenRotator = new GithubTokenRotator(
      cache,
      process.env['CROWD_GITHUB_PERSONAL_ACCESS_TOKENS'].split(','),
    )

    const token = await tokenRotator.getToken()

    const response = await fetch(`https://api.github.com/users/${username}`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    })
    if (!response.ok) {
      console.error('HTTP error', response.status)
      if (response.status === 429) {
        throw new Error('Rate limit exceeded')
      }
      return null
    } else {
      await tokenRotator.updateTokenInfo(
        token,
        Number.parseInt(response.headers.get('x-ratelimit-remaining')),
        Number.parseInt(response.headers.get('x-ratelimit-reset')),
      )

      const jsonResponse = (await response.json()) as IGithubUser
      if (jsonResponse?.login === username) {
        return jsonResponse?.id.toString()
      }

      return null
    }
  } catch (error) {
    svc.log.error('Fetch error when getting github user id!', error)
    return null
  }
}
