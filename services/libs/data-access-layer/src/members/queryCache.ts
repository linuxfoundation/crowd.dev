import { createHash, randomBytes } from 'crypto'

import { getServiceLogger } from '@crowd/logging'
import { RedisCache, RedisClient } from '@crowd/redis'
import { PageData } from '@crowd/types'

import { IDbMemberData } from './types'

interface IncludeOptions {
  identities?: boolean
  segments?: boolean
  memberOrganizations?: boolean
  onlySubProjects?: boolean
  maintainers?: boolean
}

const log = getServiceLogger()

export class MemberQueryCache {
  private cache: RedisCache
  private countCache: RedisCache
  private lockCache: RedisCache

  constructor(redis: RedisClient) {
    this.cache = new RedisCache('members-advanced', redis, log)
    this.countCache = new RedisCache('members-count', redis, log)
    this.lockCache = new RedisCache('members-refresh-lock', redis, log)
  }

  // Returns true if lock was acquired (no other refresh in progress for this key).
  // Uses a cryptographically random token to distinguish "we set it" from "already existed".
  // TTL ensures the lock auto-expires if the refresh crashes without releasing it.
  async tryAcquireRefreshLock(cacheKey: string, ttlSeconds = 90): Promise<boolean> {
    try {
      const token = randomBytes(16).toString('hex')
      const stored = await this.lockCache.setIfNotExistsOrGet(cacheKey, token, ttlSeconds)
      return stored === token
    } catch {
      return true // fail open: if Redis is down, let the refresh proceed
    }
  }

  async releaseRefreshLock(cacheKey: string): Promise<void> {
    try {
      await this.lockCache.delete(cacheKey)
    } catch {
      // best effort
    }
  }

  buildCacheKey(params: {
    fields?: string[]
    filter?: Record<string, unknown>
    include?: IncludeOptions
    includeAllAttributes?: boolean
    limit?: number
    offset?: number
    orderBy?: string
    search?: string
    segmentId?: string
  }): string {
    const cleanParams = Object.fromEntries(
      Object.entries({
        fields: params.fields?.sort(),
        filter: params.filter,
        include: params.include,
        includeAllAttributes: params.includeAllAttributes,
        limit: params.limit,
        offset: params.offset,
        orderBy: params.orderBy,
        search: params.search,
        segmentId: params.segmentId,
      }).filter(([, value]) => value !== null && value !== undefined),
    )

    const hash = createHash('md5').update(JSON.stringify(cleanParams)).digest('hex')

    const filterId = (params.filter?.id as Record<string, unknown>)?.eq

    if (filterId && typeof filterId === 'string') {
      return `members_advanced:${filterId}:${hash}`
    }

    return `members_advanced:${hash}`
  }

  buildCountCacheKey(params: {
    filter?: Record<string, unknown>
    search?: string
    segmentId?: string
  }): string {
    return this.buildCacheKey({
      filter: params.filter,
      search: params.search,
      segmentId: params.segmentId,
    })
  }

  async get(cacheKey: string): Promise<PageData<IDbMemberData> | null> {
    try {
      const cachedResult = await this.cache.get(cacheKey)
      if (cachedResult) {
        return JSON.parse(cachedResult)
      }
      return null
    } catch (error) {
      log.warn('Error retrieving from cache', { error })
      return null
    }
  }

  async set(cacheKey: string, result: PageData<IDbMemberData>, ttlSeconds: number): Promise<void> {
    try {
      await this.cache.set(cacheKey, JSON.stringify(result), ttlSeconds)
    } catch (error) {
      log.warn('Error saving to cache', { error })
    }
  }

  async getCount(cacheKey: string): Promise<number | null> {
    try {
      const cachedCount = await this.countCache.get(cacheKey)
      if (!cachedCount) return null
      const parsed = parseInt(cachedCount, 10)
      return isNaN(parsed) ? null : parsed
    } catch (error) {
      log.warn('Error retrieving count from cache', { error })
      return null
    }
  }

  async setCount(cacheKey: string, count: number, ttlSeconds: number): Promise<void> {
    try {
      await this.countCache.set(cacheKey, count.toString(), ttlSeconds)
    } catch (error) {
      log.warn('Error saving count to cache', { error })
    }
  }

  async invalidateAll(): Promise<void> {
    try {
      const [resultsDeleted, countsDeleted, locksDeleted] = await Promise.all([
        this.cache.deleteAll(),
        this.countCache.deleteAll(),
        this.lockCache.deleteAll(),
      ])
      log.info(
        `Invalidated member query cache: ${resultsDeleted} result entries, ${countsDeleted} count entries, ${locksDeleted} locks`,
      )
    } catch (error) {
      log.warn('Error invalidating member query cache', { error })
    }
  }

  async invalidateByPattern(pattern: string): Promise<void> {
    try {
      const [resultsDeleted, countsDeleted, locksDeleted] = await Promise.all([
        this.cache.deleteByKeyPattern(pattern),
        this.countCache.deleteByKeyPattern(pattern),
        this.lockCache.deleteByKeyPattern(pattern),
      ])
      log.info(
        `Invalidated member query cache by pattern: ${resultsDeleted} result entries, ${countsDeleted} count entries, ${locksDeleted} locks deleted for pattern ${pattern}`,
      )
    } catch (error) {
      log.warn('Error invalidating member query cache by pattern', { error, pattern })
    }
  }
}
