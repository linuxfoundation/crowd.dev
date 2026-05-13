import { MemberQueryCache } from '@crowd/data-access-layer'
import { getServiceLogger } from '@crowd/logging'
import { RedisClient } from '@crowd/redis'

const logger = getServiceLogger()

export async function invalidateMemberQueryCache(
  redis: RedisClient,
  memberIds?: string[],
  invalidateAll = false,
): Promise<void> {
  try {
    const cache = new MemberQueryCache(redis)

    // Normalize memberIds to a validated array of strings
    const validMemberIds: string[] = Array.isArray(memberIds)
      ? memberIds.filter((id): id is string => typeof id === 'string' && id.length > 0)
      : []

    if (validMemberIds.length > 0) {
      // Invalidate specific member cache entries (queries with filter.id.eq)
      for (const memberId of validMemberIds) {
        await cache.invalidateByPattern(`members_advanced:${memberId}:*`)
      }

      logger.debug(`Invalidated member query cache for ${validMemberIds.length} specific members`)

      // Only invalidate all caches if explicitly requested
      // This is useful for operations like update/delete that affect list views
      if (invalidateAll) {
        await cache.invalidateAll()
        logger.debug('Invalidated all member query cache entries')
      }
    } else if (invalidateAll) {
      // No valid member IDs but invalidateAll requested - invalidate all cache entries
      await cache.invalidateAll()
      logger.debug('Invalidated all member query cache')
    }
    // If no valid member IDs and invalidateAll is false, skip invalidation entirely
  } catch (error) {
    // Don't fail the operation if cache invalidation fails
    logger.warn('Failed to invalidate member query cache', { error })
  }
}
