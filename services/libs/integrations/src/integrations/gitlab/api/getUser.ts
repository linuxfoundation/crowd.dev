import { Gitlab, SimpleUserSchema, UserSchema } from '@gitbeaker/rest'

import type { IProcessStreamContext } from '../../../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'

const GITLAB_API_BASE_URL = 'https://gitlab.com/api/v4'
// user id of this user: https://gitlab.com/ghost1
// it acts instead of deleted users
export const GITLAB_GHOST_USER_ID = 1243277

export const getUser = async (
  api: InstanceType<typeof Gitlab>,
  userId: number,
  ctx: IProcessStreamContext,
): Promise<UserSchema> => {
  const cacheKey = `gitlab:user:${userId}`
  const cachedUser = await ctx.cache.get(cacheKey)

  if (cachedUser) {
    return JSON.parse(cachedUser) as UserSchema
  }

  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getUser',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let user: UserSchema

  try {
    await semaphore.acquire()
    user = (await api.Users.show(userId)) as UserSchema
  } catch (error) {
    ctx.log.error(error, 'Failed to get user', userId)
    throw handleGitlabError(error, `getUser:${userId}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  await ctx.cache.set(cacheKey, JSON.stringify(user), 24 * 60 * 60) // TTL set for one day (24 hours)

  return user as UserSchema
}

export const getUserByUsername = async (
  token: string,
  username: string,
  ctx: IProcessStreamContext,
): Promise<UserSchema> => {
  username = username.toLowerCase().trim()
  const cacheKey = `gitlab:user:username:${username}`
  const cachedUser = await ctx.cache.get(cacheKey)

  if (cachedUser) {
    return JSON.parse(cachedUser) as UserSchema
  }

  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getUserByUsername',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let users: SimpleUserSchema[]

  try {
    await semaphore.acquire()

    // Direct GitLab API call
    const response = await fetch(
      `${GITLAB_API_BASE_URL}/users?username=${encodeURIComponent(username)}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      },
    )

    if (!response.ok) {
      const error = new Error(`GitLab API error: ${response.status} ${response.statusText}`)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ;(error as any).response = {
        status: response.status,
        statusText: response.statusText,
        headers: Object.fromEntries(response.headers.entries()),
        data: await response.json().catch(() => ({})),
      }
      throw error
    }

    users = (await response.json()) as SimpleUserSchema[]
  } catch (error) {
    throw handleGitlabError(error, `getUserByUsername:${username}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  // iterate over users and return the first one that has the same username
  const user = users.find((u) => u.username.trim().toLowerCase() === username)

  let userId: number
  if (user) {
    userId = user.id
  } else {
    userId = GITLAB_GHOST_USER_ID
  }

  // Create a temporary Gitlab instance to fetch the full user
  const api = new Gitlab({ oauthToken: token })
  const fullUser = await getUser(api, userId, ctx)

  await ctx.cache.set(cacheKey, JSON.stringify(fullUser), 24 * 60 * 60) // TTL set for one day (24 hours)

  return fullUser
}
