import commandLineArgs from 'command-line-args'

import {
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
} from '@crowd/common_services'
import { getServiceLogger } from '@crowd/logging'
import { getRedisClient, stopClient } from '@crowd/redis'

import { REDIS_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'confirm',
    alias: 'c',
    type: Boolean,
    description: 'Actually delete old hash keys. Defaults to dry-run.',
  },
  {
    name: 'count',
    type: Number,
    defaultValue: 500,
    description: 'SCAN count hint.',
  },
]

const parameters = commandLineArgs(options)

function memberIdFromDatesKey(key: string): string {
  return key.slice(`${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:`.length)
}

setImmediate(async () => {
  const dryRun = !parameters.confirm
  const scanCount = parameters.count
  const redis = await getRedisClient(REDIS_CONFIG, true)
  const pattern = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:*`

  let scanned = 0
  let hashKeys = 0
  let deleted = 0
  let cursor = 0

  log.info({ dryRun, pattern, scanCount }, 'Removing old member organization stint hash keys.')

  try {
    do {
      const result = await redis.scan(cursor, {
        MATCH: pattern,
        COUNT: scanCount,
      })

      cursor = Number(result.cursor)
      scanned += result.keys.length

      for (const key of result.keys) {
        const type = await redis.type(key)
        if (type === 'hash') {
          hashKeys++
          const memberId = memberIdFromDatesKey(key)

          if (dryRun) {
            log.info({ key, memberId }, 'Would remove old hash key and queue member.')
          } else {
            await redis.multi().del(key).sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId).exec()

            deleted++
          }
        }
      }
    } while (cursor !== 0)

    log.info({ dryRun, scanned, hashKeys, deleted }, 'Finished removing old hash keys.')
  } finally {
    await stopClient(redis)
  }

  process.exit(0)
})
