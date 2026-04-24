import CronTime from 'cron-time-generator'
import { 
  inferMemberOrganizationStintChanges, 
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX, 
  MEMBER_ORG_STINT_CHANGES_QUEUE 
} from '@crowd/common_services'
import {
  createMemberOrganization,
  fetchMemberOrganizationsBySource,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx, QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { REDIS_CONFIG, RedisClient, getRedisClient } from '@crowd/redis'
import { MemberOrgDate, MemberOrgStintChange, OrganizationSource } from '@crowd/types'
import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'infer-member-organization-stint-changes',
  cronTime: CronTime.every(5).minutes(),
  timeout: 10 * 60,
  process: async (ctx) => {
    const redis = await getRedisClient(REDIS_CONFIG())
    const db = await getDbConnection(WRITE_DB_CONFIG(), 2, 0)
    const qx = pgpQx(db)

    // 1. Fetch a batch of work triggers (Member IDs)
    const memberIds = await redis.sPop(MEMBER_ORG_STINT_CHANGES_QUEUE, 500)
    if (!memberIds?.length) return

    ctx.log.info({ count: memberIds.length }, 'Processing pending members.')
    const stats = { processed: 0, inserts: 0, updates: 0 }

    for (const memberId of memberIds) {
      try {
        // 2. Get the activity dates for this member
        const activityDates = await popMemberOrganizationActivityDates(redis, memberId)
        
        // If the member has no activity dates, move to the next member
        if (activityDates.length === 0) continue

        // 3. Sync with existing database state
        const existingOrgs = await fetchMemberOrganizationsBySource(
          qx,
          memberId,
          OrganizationSource.EMAIL_DOMAIN,
        )

        // 4. Calculate required stint changes
        const stintChanges = inferMemberOrganizationStintChanges(
          memberId,
          existingOrgs,
          activityDates,
        )

        if (stintChanges.length === 0) continue

        const counts = {
          inserts: stintChanges.filter((c) => c.type === 'insert').length,
          updates: stintChanges.filter((c) => c.type === 'update').length,
        }

        ctx.log.info({ memberId, ...counts }, 'Stint changes identified.')

        ctx.log.debug(
          { memberId, activityDates, existingOrgs, stintChanges },
          'Stint inference trace.',
        )

        // @todo: Enable writes after dry-run validation
        // await applyStintChanges(qx, stintChanges) 

        stats.processed++
        stats.inserts += counts.inserts
        stats.updates += counts.updates
      } catch (err) {
        ctx.log.error(err, { memberId }, 'Failed to process member stint inference.')
      }
    }

    ctx.log.info(stats, 'Batch inference complete.')
  },
}

async function popMemberOrganizationActivityDates(
  redis: RedisClient,
  memberId: string,
): Promise<MemberOrgDate[]> {
  const key = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`
  
  // hGetAll + del in a multi block makes the "Pop" atomic for the entire Hash
  const [hash] = (await redis.multi().hGetAll(key).del(key).exec()) as [Record<string, string> | null, number]

  if (!hash || Object.keys(hash).length === 0) return []

  return Object.entries(hash)
    .flatMap(([organizationId, datesJson]) =>
      (JSON.parse(datesJson) as string[]).map((date) => ({ organizationId, date })),
    )
    .sort((a, b) => a.date.localeCompare(b.date))
}

async function applyStintChanges(
  qx: QueryExecutor,
  stintChanges: MemberOrgStintChange[],
): Promise<void> {
  for (const change of stintChanges) {
    if (change.type === 'insert') {
      await createMemberOrganization(qx, change.memberId, {
        organizationId: change.organizationId,
        dateStart: change.dateStart,
        dateEnd: change.dateEnd,
        source: OrganizationSource.EMAIL_DOMAIN,
      })
      continue
    }

    if (!change.id) {
      throw new Error('Missing id for update stint change.')
    }

    await updateMemberOrganization(qx, change.memberId, change.id, {
      dateStart: change.dateStart,
      dateEnd: change.dateEnd,
    })
  }
}

export default job