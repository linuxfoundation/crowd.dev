import { Logger } from '@crowd/logging'
import { RedisClient, createChangeClock } from '@crowd/redis'

// Cached organization lookups (data-sink-worker) live shorter than this, so an expired mark can
// only belong to a lookup that has expired too
export const ORGANIZATION_CHANGE_RETENTION_SECONDS = 60 * 60

// The Redis client has no command timeout and queues commands while it reconnects
const MARK_DEADLINE_MS = 2_000

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
    let timer: NodeJS.Timeout | undefined
    const deadline = new Promise<'deadline'>((resolve) => {
      timer = setTimeout(() => resolve('deadline'), MARK_DEADLINE_MS)
    })
    // a mark still queued at the deadline lands once Redis answers, which only drops more lookups
    const marking = organizationChangeClock(redis)
      .markChanged(ids)
      .then(() => 'marked' as const)
    marking.catch(() => undefined)
    try {
      if ((await Promise.race([marking, deadline])) === 'deadline') {
        log.warn({ organizationIds: ids }, 'Redis did not answer in time to mark organizations!')
      }
      return
    } catch (err) {
      if (attempt >= 2) {
        log.warn({ err, organizationIds: ids }, 'Failed to mark organizations as changed!')
        return
      }
    } finally {
      clearTimeout(timer)
    }
  }
}
