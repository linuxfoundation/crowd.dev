import { Logger } from '@crowd/logging'
import { RedisClient, createChangeClock } from '@crowd/redis'

// Cached organization lookups (data-sink-worker) live shorter than this, so an expired mark can
// only belong to a lookup that has expired too
export const ORGANIZATION_CHANGE_RETENTION_SECONDS = 60 * 60

export const organizationChangeClock = (redis: RedisClient) =>
  createChangeClock(redis, 'organizations', ORGANIZATION_CHANGE_RETENTION_SECONDS)

// Call it after the write commits, for every organization whose identities, attributes or existence
// changed. A mark that never lands leaves cached lookups of these organizations valid until they expire
export async function markOrganizationsChanged(
  redis: RedisClient | undefined,
  organizationIds: (string | undefined | null)[],
  log: Logger,
): Promise<void> {
  const ids = Array.from(new Set(organizationIds.filter((id): id is string => !!id)))
  if (ids.length === 0) {
    return
  }
  if (!redis) {
    log.warn({ organizationIds: ids }, 'No Redis client to mark changed organizations with!')
    return
  }
  for (let attempt = 1; ; attempt++) {
    try {
      await organizationChangeClock(redis).markChanged(ids)
      return
    } catch (err) {
      if (attempt >= 2) {
        log.warn({ err, organizationIds: ids }, 'Failed to mark organizations as changed!')
        return
      }
    }
  }
}
