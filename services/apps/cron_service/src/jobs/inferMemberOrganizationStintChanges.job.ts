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
import { REDIS_CONFIG, RedisCache, getRedisClient } from '@crowd/redis'
import { MemberOrgDate, MemberOrgStintChange, OrganizationSource } from '@crowd/types'

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
        const rawMembers = await redis.sMembers(datesKey)

        if (!rawMembers?.length) {
          await redis.sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId)
          continue
        }

        const orgDates = parseSetMembers(rawMembers)

        if (orgDates.length > 0) {
          const existingOrgs = await fetchMemberOrganizationsBySource(
            qx,
            memberId,
            OrganizationSource.EMAIL_DOMAIN,
          )

          const changes = inferMemberOrganizationStintChanges(memberId, existingOrgs, orgDates)

          if (changes.length > 0) {
            ctx.log.debug({ memberId, changes }, 'Stint changes identified.')
            await qx.tx((tx) => applyStintChanges(tx, changes))
          }
        }

        // Atomically remove only the values we read.
        // If no new values were added, remove the member from the queue.
        await RedisCache.ackSetMembers(
          redis,
          datesKey,
          MEMBER_ORG_STINT_CHANGES_QUEUE,
          memberId,
          rawMembers,
        )

        processed++
      } catch (err) {
        ctx.log.error(err, { memberId }, 'Failed to process member stint inference.')
        throw err
      }
    }

    ctx.log.info({ processed }, 'Batch complete.')
  },
}

/**
 * Parses set members of the form "orgId|date" into typed activity dates.
 */
function parseSetMembers(members: string[]): MemberOrgDate[] {
  const results: MemberOrgDate[] = []

  for (const m of members) {
    const idx = m.indexOf('|')
    if (idx > 0) {
      results.push({ organizationId: m.slice(0, idx), date: m.slice(idx + 1) })
    }
  }

  return results
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
