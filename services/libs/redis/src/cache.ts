import { Logger, LoggerBase } from '@crowd/logging'
import { ICache } from '@crowd/types'

import type { RedisClient } from './types'

export class RedisCache extends LoggerBase implements ICache {
  private readonly prefixer: (key: string) => string

  private readonly prefixRegex: RegExp

  private readonly directory: string

  constructor(
    public readonly name: string,
    private readonly client: RedisClient,
    parentLog: Logger,
  ) {
    super(parentLog, { cacheName: name })

    this.directory = `cache_${name}`
    this.prefixRegex = new RegExp(`^${this.directory}:`)

    this.prefixer = (key: string) => `${this.directory}:${key}`
  }

  public getDirectory(): string {
    return this.directory
  }

  async get(key: string): Promise<string> {
    const actualKey = this.prefixer(key)
    const value = await this.client.get(actualKey)
    return value
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    const actualKey = this.prefixer(key)

    if (ttlSeconds !== undefined) {
      await this.client.setEx(actualKey, ttlSeconds, value)
    } else {
      await this.client.set(actualKey, value)
    }
  }

  async exists(key: string): Promise<boolean> {
    const actualKey = this.prefixer(key)
    const value = await this.client.exists(actualKey)
    return value === 1
  }

  async increment(key: string, incrementBy = 1, ttlSeconds?: number): Promise<number> {
    const actualKey = this.prefixer(key)

    if (ttlSeconds !== undefined) {
      const [incrResult] = await this.client
        .multi()
        .incrBy(actualKey, incrementBy)
        .expire(actualKey, ttlSeconds)
        .exec()
      return incrResult as number
    }

    const result = await this.client.incrBy(actualKey, incrementBy)
    return result
  }

  async decrement(key: string, decrementBy = 1, ttlSeconds?: number): Promise<number> {
    const actualKey = this.prefixer(key)

    if (ttlSeconds !== undefined) {
      const [decrResult] = await this.client
        .multi()
        .decrBy(actualKey, decrementBy)
        .expire(actualKey, ttlSeconds)
        .exec()
      return decrResult as number
    }

    const result = await this.client.decrBy(actualKey, decrementBy)
    return result
  }

  public setIfNotExistsAlready(key: string, value: string): Promise<boolean> {
    const actualKey = this.prefixer(key)
    return this.client.setNX(actualKey, value)
  }

  public async delete(key: string): Promise<number> {
    const actualKey = this.prefixer(key)
    return this.client.del(actualKey)
  }

  public async hget(key: string, field: string): Promise<string> {
    const actualKey = this.prefixer(key)
    return this.client.hGet(actualKey, field)
  }

  public async hset(key: string, field: string, value: string): Promise<number> {
    const actualKey = this.prefixer(key)
    return this.client.hSet(actualKey, field, value)
  }

  public async hgetall(key: string): Promise<{ [key: string]: string }> {
    const actualKey = this.prefixer(key)
    return this.client.hGetAll(actualKey)
  }

  private async deleteByPattern(pattern: string): Promise<number> {
    const script = `
local delpattern = ARGV[1]
local limit = 5000
local count = 0
local valuelist = redis.call('keys', delpattern)
if valuelist then
  if #valuelist ~= 0 then
    for i=1,#valuelist,limit do
      local tempCount = redis.call('del', unpack(valuelist, i, math.min(i+limit-1, #valuelist)))
      count = count + tempCount
    end
  end
end
return count`

    const result = await this.client.eval(script, {
      arguments: [pattern],
    })

    return result as number
  }

  public deleteByKeyPattern(keyPattern: string): Promise<number> {
    const actualPattern = this.prefixer(keyPattern)
    return this.deleteByPattern(actualPattern)
  }

  public deleteAll(): Promise<number> {
    return this.deleteByPattern(this.prefixer('*'))
  }

  public async getKeys(pattern: string, removeCacheName = true): Promise<string[]> {
    const actualPattern = this.prefixer(pattern)
    const keys = await this.client.keys(actualPattern)

    if (removeCacheName) {
      return keys.map((k) => k.replace(this.prefixRegex, ''))
    }

    return keys
  }

  public getAllValues(): Promise<Map<string, string>> {
    return this.getValueByKeyPattern('*')
  }

  public async getValueByKeyPattern(
    keyPattern: string,
    removeCacheName = true,
  ): Promise<Map<string, string>> {
    const actualPattern = this.prefixer(keyPattern)
    const script = `
local keypattern = ARGV[1]
local limit = 5000

local valuelist = redis.call('keys', keypattern)
local results = {}

results['keys'] = {}
results['values'] = {}

local values = results['values']

if valuelist then
  if #valuelist ~= 0 then
    results['keys'] = valuelist
    for i=1,#valuelist,limit do
      local tmp = redis.call('mget', unpack(valuelist, i, math.min(i+limit-1, #valuelist)))
      for j=1, #tmp do
        values[#values+1] = tmp[j]
      end
    end
  end
end
return cjson.encode(results)`

    const results = await this.client.eval(script, {
      arguments: [actualPattern],
    })

    const json: { keys: string[]; values: string[] } = JSON.parse(results as string)
    const map = new Map<string, string>()
    if (json.keys !== undefined && Array.isArray(json.keys)) {
      json.keys.forEach((key, index) => {
        if (removeCacheName) {
          map.set(key.replace(this.prefixRegex, ''), json.values[index])
        } else {
          map.set(key, json.values[index])
        }
      })
    }

    return map
  }

  /**
   * Atomically removes members from a set and cleans up the tracking queue
   * if the set is now empty. Prevents race conditions between SCARD and SREM.
   *
   * @returns 1 if the queue entry was removed, 0 otherwise.
   */
  public static async ackSetMembers(
    client: RedisClient,
    setKey: string,
    queueKey: string,
    queueMember: string,
    members: string[],
  ): Promise<number> {
    // Guard clause: Redis errors if you call SREM with no members
    if (members.length === 0) return 0

    const script = `
      local chunkSize = 500
      for i = 2, #ARGV, chunkSize do
        redis.call('SREM', KEYS[1], unpack(ARGV, i, math.min(i + chunkSize - 1, #ARGV)))
      end

      if redis.call('SCARD', KEYS[1]) == 0 then
        return redis.call('SREM', KEYS[2], ARGV[1])
      end
      return 0
    `

    const result = await client.eval(script, {
      keys: [setKey, queueKey],
      arguments: [queueMember, ...members],
    })

    return Number(result)
  }

  public async setIfNotExistsOrGet(
    key: string,
    value: string,
    ttlSeconds?: number,
  ): Promise<string> {
    const actualKey = this.prefixer(key)

    const script = `
local key = KEYS[1]
local value = ARGV[1]
local ttl = ARGV[2]

-- Try to set the key if it doesn't exist
local wasSet = redis.call('setnx', key, value)

if wasSet == 1 then
  -- If we set it and TTL was provided, set the expiration
  if ttl ~= '' then
    redis.call('expire', key, ttl)
  end
  return value
else
  -- If we didn't set it, get the existing value
  return redis.call('get', key)
end`

    const result = await this.client.eval(script, {
      keys: [actualKey],
      arguments: [value, ttlSeconds?.toString() ?? ''],
    })

    return result as string
  }
}
