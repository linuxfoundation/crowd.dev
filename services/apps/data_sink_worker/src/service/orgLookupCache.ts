import { createHash } from 'crypto'

import {
  ORGANIZATION_CHANGE_RETENTION_SECONDS,
  markOrganizationsChanged,
  organizationChangeClock,
} from '@crowd/common_services'
import { IFindOrCreateOrganizationResult } from '@crowd/data-access-layer'
import { Logger } from '@crowd/logging'
import { RedisClient } from '@crowd/redis'

// Shorter than the change marks are kept, so a lookup never outlives the mark that invalidates it
const TTL_SECONDS = Math.min(5 * 60, ORGANIZATION_CHANGE_RETENTION_SECONDS / 2)

// Valid while the organization was not marked after seenAt. A clock behind seenAt was reset
// (failover, flush), so later marks can be lower than it. A miss returns the clock to start from
const FIND = `
local clock = tonumber(redis.call('get', KEYS[2])) or 0
local entry = redis.call('get', KEYS[1])
if not entry then
  return clock
end
local sep = string.find(entry, ':', 1, true)
local seenAt = sep and tonumber(string.sub(entry, sep + 1))
if not seenAt or seenAt > clock then
  return clock
end
local organizationId = string.sub(entry, 1, sep - 1)
if (tonumber(redis.call('get', ARGV[1] .. organizationId)) or 0) > seenAt then
  return clock
end
return organizationId`

// Shared by every data-sink pod: which organization a payload went to, for payloads applied before
// with nothing to change. While Redis is not ready every call is a miss or a no-op, never a wait
export interface IOrgLookupCache {
  // startedAt: the clock to look up with when there is no hit
  find(payloadKey: string): Promise<{ organizationId?: string; startedAt?: number }>
  start(): Promise<number | undefined>
  remember(
    payloadKey: string,
    result: IFindOrCreateOrganizationResult,
    startedAt: number | undefined,
  ): Promise<void>
  markChanged(organizationIds: string[]): Promise<void>
}

// The Redis client has no command timeout, and a lookup must never wait long on a cache
const DEADLINE_MS = 1_000

async function withinDeadline<T>(work: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error('Redis did not answer in time')), DEADLINE_MS)
  })
  work.catch(() => undefined)
  try {
    return await Promise.race([work, deadline])
  } finally {
    clearTimeout(timer)
  }
}

export function createOrgLookupCache(redis: RedisClient, log: Logger): IOrgLookupCache {
  const clock = organizationChangeClock(redis)
  const lookupKey = (payloadKey: string) =>
    `org-lookup:{organizations}:${createHash('sha256').update(payloadKey).digest('base64')}`

  return {
    find: async (payloadKey) => {
      if (!redis.isReady) {
        return {}
      }
      try {
        const found = await withinDeadline(
          redis.eval(FIND, {
            keys: [lookupKey(payloadKey), clock.clockKey()],
            arguments: [clock.changedKeyPrefix()],
          }),
        )
        return typeof found === 'string' ? { organizationId: found } : { startedAt: Number(found) }
      } catch (err) {
        log.warn({ err }, 'Failed to look up a cached organization!')
        return {}
      }
    },

    start: async () => {
      if (!redis.isReady) {
        return undefined
      }
      try {
        return await withinDeadline(clock.now())
      } catch (err) {
        log.warn({ err }, 'Failed to read the organization change clock!')
        return undefined
      }
    },

    // A write is marked, never cached: two pods can create the same organization at once, and
    // the loser's identities are dropped, so only a lookup that changed nothing is known to hold
    remember: async (payloadKey, result, startedAt) => {
      if (result.changed) {
        if (redis.isReady) {
          await markOrganizationsChanged(redis, [result.id], log)
        }
        return
      }
      if (!redis.isReady || startedAt === undefined) {
        return
      }
      try {
        await withinDeadline(
          redis.set(lookupKey(payloadKey), `${result.id}:${startedAt}`, { EX: TTL_SECONDS }),
        )
      } catch (err) {
        log.warn({ err, organizationId: result.id }, 'Failed to cache an organization lookup!')
      }
    },

    markChanged: async (organizationIds) => {
      if (redis.isReady) {
        await markOrganizationsChanged(redis, organizationIds, log)
      }
    },
  }
}
