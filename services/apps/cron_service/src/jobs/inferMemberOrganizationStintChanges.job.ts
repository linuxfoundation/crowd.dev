import CronTime from 'cron-time-generator'

import {
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
  inferMemberOrganizationStintChanges,
} from '@crowd/common_services'
import { fetchMemberOrganizationsBySource } from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { REDIS_CONFIG, getRedisClient } from '@crowd/redis'
import { OrganizationSource } from '@crowd/types'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'infer-member-organization-stint-changes',
  cronTime: CronTime.every(5).minutes(),
  timeout: 10 * 60,
  process: async (ctx) => {
    const redis = await getRedisClient(REDIS_CONFIG())
    const db = await getDbConnection(WRITE_DB_CONFIG(), 2, 0)
    const qx = pgpQx(db)

    // 1. Get a batch of work
    const memberIds = await redis.sRandMember(MEMBER_ORG_STINT_CHANGES_QUEUE, 500)
    if (!memberIds?.length) return

    ctx.log.info({ count: memberIds.length }, 'Processing pending members.')
    const stats = { processed: 0, inserts: 0, updates: 0 }

    for (const memberId of memberIds) {
      try {
        const datesKey = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`
        const hash = await redis.hGetAll(datesKey)

        // If no data, just remove from queue and move on
        if (!hash || Object.keys(hash).length === 0) {
          await redis.sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId)
          continue
        }

        // 2. Parse Redis data into domain objects
        const { activityDates, orgIds } = parseMemberActivityHash(hash)

        if (activityDates.length > 0) {
          // 3. Compare with DB and calculate delta
          const existingOrgs = await fetchMemberOrganizationsBySource(
            qx,
            memberId,
            OrganizationSource.EMAIL_DOMAIN,
          )

          const changes = inferMemberOrganizationStintChanges(memberId, existingOrgs, activityDates)

          if (changes.length > 0) {
            ctx.log.info({ memberId, count: changes.length }, 'Stint changes identified.')
            stats.inserts += changes.filter((c) => c.type === 'insert').length
            stats.updates += changes.filter((c) => c.type === 'update').length
          }
        }

        // 4. Cleanup: Remove only the fields we actually read
        await redis
          .multi()
          .hDel(datesKey, ...orgIds)
          .sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId)
          .exec()

        stats.processed++
      } catch (err) {
        ctx.log.error(err, { memberId }, 'Failed to process member stint inference.')
      }
    }

    ctx.log.info(stats, 'Batch complete.')
  },
}

/**
 * Parses the Redis hash into a clean, typed list of activity dates.
 */
function parseMemberActivityHash(hash: Record<string, string>) {
  const orgIds = Object.keys(hash)
  const activityDates = orgIds
    .flatMap((organizationId) => {
      try {
        const dates = JSON.parse(hash[organizationId])
        return Array.isArray(dates)
          ? dates
              .filter((d): d is string => typeof d === 'string')
              .map((date) => ({ organizationId, date }))
          : []
      } catch {
        return []
      }
    })
    .sort((a, b) => a.date.localeCompare(b.date))

  return { activityDates, orgIds }
}

export default job
