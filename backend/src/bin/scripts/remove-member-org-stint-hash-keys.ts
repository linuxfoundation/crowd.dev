import {
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
} from '@crowd/common_services'
import { getServiceLogger } from '@crowd/logging'
import { getRedisClient, stopClient } from '@crowd/redis'

import { REDIS_CONFIG } from '@/conf'

const log = getServiceLogger()

setImmediate(async () => {
  const redis = await getRedisClient(REDIS_CONFIG, true)
  const prefixes = [MEMBER_ORG_STINT_CHANGES_QUEUE, `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:*`]

  try {
    for (const pattern of prefixes) {
      let cursor = 0
      log.info({ pattern }, 'Nuking keys matching pattern.')

      do {
        const result = await redis.scan(cursor, { MATCH: pattern, COUNT: 1000 })
        cursor = Number(result.cursor)

        if (result.keys.length > 0) {
          await redis.del(result.keys)
          log.info({ count: result.keys.length }, 'Deleted batch of keys.')
        }
      } while (cursor !== 0)
    }

    log.info('Cleanup complete.')
  } finally {
    await stopClient(redis)
  }
})
