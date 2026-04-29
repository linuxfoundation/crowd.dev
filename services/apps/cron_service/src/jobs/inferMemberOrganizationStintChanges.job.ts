import CronTime from 'cron-time-generator'

import {
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
  inferMemberOrganizationStintChanges,
} from '@crowd/common_services'
import {
  QueryExecutor,
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicy,
  createMemberOrganization,
  fetchMemberOrganizationsBySource,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { REDIS_CONFIG, getRedisClient } from '@crowd/redis'
import { MemberOrgStintChange, OrganizationSource } from '@crowd/types'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'infer-member-organization-stint-changes',
  cronTime: CronTime.every(5).minutes(),
  timeout: 10 * 60,
  process: async (ctx) => {
    const redis = await getRedisClient(REDIS_CONFIG())
    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    ctx.log.info('Starting member organization stint inference job.')

    const memberIds = await redis.sRandMemberCount(MEMBER_ORG_STINT_CHANGES_QUEUE, 500)
    if (!memberIds?.length) return

    ctx.log.info({ count: memberIds.length }, 'Processing members from queue.')

    let processed = 0

    for (const memberId of memberIds) {
      try {
        const datesKey = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`
        const hash = await redis.hGetAll(datesKey)

        if (!hash || Object.keys(hash).length === 0) {
          await redis.sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId)
          continue
        }

        const { activityDates, orgIds } = parseMemberActivityHash(hash)

        if (activityDates.length > 0) {
          const existingOrgs = await fetchMemberOrganizationsBySource(
            qx,
            memberId,
            OrganizationSource.EMAIL_DOMAIN,
          )

          const changes = inferMemberOrganizationStintChanges(memberId, existingOrgs, activityDates)

          if (changes.length > 0) {
            ctx.log.debug({ memberId, changes }, 'Stint changes identified.')
            await applyStintChanges(qx, changes)
          }
        }

        // Remove only the fields we actually read
        await redis
          .multi()
          .hDel(datesKey, orgIds)
          .sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId)
          .exec()

        processed++
      } catch (err) {
        ctx.log.error(err, { memberId }, 'Failed to process member stint inference.')
      }
    }

    ctx.log.info({ processed }, 'Batch complete.')
  },
}

/**
 * Parses the Redis hash into a clean, typed list of activity dates.
 */
function parseMemberActivityHash(hash: Record<string, string>) {
  const orgIds = Object.keys(hash)
  const activityDates = orgIds.flatMap((organizationId) => {
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
  return { activityDates, orgIds }
}

/**
 * Applies the stint changes to the database.
 */
async function applyStintChanges(qx: QueryExecutor, changes: MemberOrgStintChange[]) {
  for (const change of changes) {
    if (change.type === 'insert') {
      const memberOrganizationId = await createMemberOrganization(qx, change.memberId, {
        organizationId: change.organizationId,
        dateStart: change.dateStart,
        dateEnd: change.dateEnd,
        source: OrganizationSource.EMAIL_DOMAIN,
      })

      const isAffiliationBlocked = await checkOrganizationAffiliationPolicy(
        qx,
        change.organizationId,
      )

      if (memberOrganizationId && isAffiliationBlocked) {
        await changeMemberOrganizationAffiliationOverrides(qx, [
          {
            memberId: change.memberId,
            memberOrganizationId,
            allowAffiliation: false,
          },
        ])
      }
    } else {
      await updateMemberOrganization(qx, change.memberId, change.id, {
        dateStart: change.dateStart,
        dateEnd: change.dateEnd,
      })
    }
  }
}

export default job
