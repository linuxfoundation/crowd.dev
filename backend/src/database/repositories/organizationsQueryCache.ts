import { createHash } from 'crypto'

import { PageData } from '@crowd/common'
import { getServiceLogger } from '@crowd/logging'
import { RedisCache, RedisClient } from '@crowd/redis'

interface IOrganizationData {
  id: string
  displayName: string
  [key: string]: any
}

interface IncludeOptions {
  aggregates?: boolean
  identities?: boolean
  lfxMemberships?: boolean
  segments?: boolean
  attributes?: boolean
}

const log = getServiceLogger()

export class OrganizationQueryCache {
  private cache: RedisCache

  private countCache: RedisCache

  constructor(redis: RedisClient) {
    this.cache = new RedisCache('organizations-advanced', redis, log)
    this.countCache = new RedisCache('organizations-count', redis, log)
  }

  static buildCacheKey(params: {
    countOnly?: boolean
    fields?: string[]
    filter?: Record<string, unknown>
    include?: IncludeOptions
    limit: number
    offset: number
    orderBy?: string
    search?: string
    segmentId?: string
  }): string {
    const cleanParams = Object.fromEntries(
      Object.entries({
        countOnly: params.countOnly,
        fields: params.fields?.sort(),
        filter: params.filter,
        include: params.include,
        limit: params.limit,
        offset: params.offset,
        orderBy: params.orderBy,
        search: params.search,
        segmentId: params.segmentId,
      }).filter(([, value]) => value !== null && value !== undefined),
    )

    const hash = createHash('md5').update(JSON.stringify(cleanParams)).digest('hex')
    return `organizations_advanced_${hash}`
  }

  async get(cacheKey: string): Promise<PageData<IOrganizationData> | null> {
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

  async set(
    cacheKey: string,
    result: PageData<IOrganizationData>,
    ttlSeconds: number,
  ): Promise<void> {
    try {
      await this.cache.set(cacheKey, JSON.stringify(result), ttlSeconds)
    } catch (error) {
      log.warn('Error saving to cache', { error })
    }
  }

  async getCount(cacheKey: string): Promise<number | null> {
    try {
      const cachedCount = await this.countCache.get(cacheKey)
      return cachedCount ? parseInt(cachedCount, 10) : null
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
}
