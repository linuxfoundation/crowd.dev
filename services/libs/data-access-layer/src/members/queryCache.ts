import { createHash } from 'crypto'

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

  constructor(redis: RedisClient) {
    this.cache = new RedisCache('members-advanced', redis, log)
    this.countCache = new RedisCache('members-count', redis, log)
  }

  buildCacheKey(params: {
    fields?: string[]
    filter?: Record<string, unknown>
    include?: IncludeOptions
    includeAllAttributes?: boolean
    limit: number
    offset: number
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
      const [resultsDeleted, countsDeleted] = await Promise.all([
        this.cache.deleteAll(),
        this.countCache.deleteAll(),
      ])
      log.info(
        `Invalidated member query cache: ${resultsDeleted} result entries, ${countsDeleted} count entries`,
      )
    } catch (error) {
      log.warn('Error invalidating member query cache', { error })
    }
  }

  async invalidateByPattern(pattern: string): Promise<void> {
    try {
      const [resultsDeleted, countsDeleted] = await Promise.all([
        this.cache.deleteByKeyPattern(pattern),
        this.countCache.deleteByKeyPattern(pattern),
      ])
      log.info(
        `Invalidated member query cache by pattern: ${resultsDeleted} result entries, ${countsDeleted} count entries deleted for pattern ${pattern}`,
      )
    } catch (error) {
      log.warn('Error invalidating member query cache by pattern', { error, pattern })
    }
  }
}
