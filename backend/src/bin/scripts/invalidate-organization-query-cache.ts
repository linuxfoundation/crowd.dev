import { getServiceLogger } from '@crowd/logging'
import { RedisCache, getRedisClient } from '@crowd/redis'

import { REDIS_CONFIG } from '@/conf'

const log = getServiceLogger()

setImmediate(async () => {
  try {
    const redis = await getRedisClient(REDIS_CONFIG, true)

    const resultsCache = new RedisCache('organizations-advanced', redis, log)
    const countCache = new RedisCache('organizations-count', redis, log)

    const [resultsDeleted, countsDeleted] = await Promise.all([
      resultsCache.deleteAll(),
      countCache.deleteAll(),
    ])

    log.info(
      { resultsDeleted, countsDeleted },
      'Invalidated organization query cache',
    )
    process.exit(0)
  } catch (error) {
    log.error('Failed to invalidate organization query cache', { error })
    process.exit(1)
  }
})
