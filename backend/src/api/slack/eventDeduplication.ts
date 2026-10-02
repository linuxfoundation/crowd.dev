import type { RedisClient } from '@crowd/redis'

const EVENT_KEY_PREFIX = 'slack_event'
const EVENT_TTL_SECONDS = 5 * 60

export async function claimSlackEvent(redis: RedisClient, eventId: string): Promise<boolean> {
  const result = await redis.set(`${EVENT_KEY_PREFIX}:${eventId}`, '1', {
    NX: true,
    EX: EVENT_TTL_SECONDS,
  })
  return result === 'OK'
}
