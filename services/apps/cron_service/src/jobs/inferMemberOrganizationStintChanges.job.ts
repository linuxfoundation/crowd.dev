import CronTime from 'cron-time-generator'

import {
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
  inferMemberOrganizationStintChanges,
  signalMemberUpdate,
} from '@crowd/common_services'
import {
  MemberField,
  QueryExecutor,
  changeMemberOrganizationAffiliationOverrides,
  createMemberOrganization,
  deleteUndatedMemberOrganizations,
  fetchManyOrganizationAffiliationPolicies,
  fetchMemberOrganizationsBySource,
  findMemberById,
  findOrgsByIds,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { deleteMemberSegmentAffiliations } from '@crowd/data-access-layer/src/member_segment_affiliations'
import { findMergedPrimaryIds } from '@crowd/data-access-layer/src/mergeActions/repo'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { Logger } from '@crowd/logging'
import { REDIS_CONFIG, RedisCache, RedisClient, getRedisClient } from '@crowd/redis'
import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'
import {
  IMemberOrganization,
  MemberOrgDate,
  MemberOrgStintChange,
  MergeActionType,
  OrganizationSource,
} from '@crowd/types'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'infer-member-organization-stint-changes',
  cronTime: CronTime.every(5).minutes(),
  timeout: 10 * 60,
  process: async (ctx) => {
    const redis = await getRedisClient(REDIS_CONFIG())

    ctx.log.info('Starting member organization stint inference job.')

    const memberIds = await redis.sRandMemberCount(MEMBER_ORG_STINT_CHANGES_QUEUE, 500)

    if (!memberIds?.length) return

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)
    const temporal = await getTemporalClient(TEMPORAL_CONFIG())

    ctx.log.info({ count: memberIds.length }, 'Processing members from queue.')

    let processed = 0

    for (const memberId of memberIds) {
      try {
        const datesKey = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`
        const rawMembers = await redis.sMembers(datesKey)

        if (!rawMembers?.length) {
          await purgeMember(redis, memberId)
          continue
        }

        // Skip if the member was hard-deleted (e.g., due to merge) after being queued.
        // This prevents memberOrganizations foreign key (FK) violations from stale Redis entries.
        const member = await findMemberById(qx, memberId, [MemberField.ID])

        if (!member) {
          ctx.log.warn({ memberId }, 'Member no longer exists, removing from queue.')

          await purgeMember(redis, memberId)
          continue
        }

        const orgDates = parseSetMembers(rawMembers)

        if (orgDates.length > 0) {
          const existingOrgs = await fetchMemberOrganizationsBySource(
            qx,
            memberId,
            OrganizationSource.EMAIL_DOMAIN,
            { withDeleted: true },
          )

          const reconciledOrgDates = await reconcileOrganizationDates(
            qx,
            existingOrgs,
            orgDates,
            memberId,
            ctx.log,
          )

          const changes = inferMemberOrganizationStintChanges(
            memberId,
            existingOrgs,
            reconciledOrgDates,
          )

          if (changes.length > 0) {
            ctx.log.debug({ memberId, changes }, 'Stint changes identified.')
            await qx.tx((tx) => applyStintChanges(tx, changes))

            ctx.log.debug(
              { memberId },
              'Triggering member update workflow to refresh affiliations.',
            )
            await signalMemberUpdate(temporal, memberId)
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
        if ((err as { code?: string })?.code === '23503') {
          const constraint = (err as { constraint?: string })?.constraint
          ctx.log.warn(
            err,
            { memberId, constraint },
            'Stint change referenced a missing related record.',
          )
          continue
        }

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

// Merged orgs are rewritten to their primary id instead of dropped; only genuinely
// deleted orgs are dropped, since those can never resolve to a valid target.
async function reconcileOrganizationDates(
  qx: QueryExecutor,
  existingOrgs: IMemberOrganization[],
  orgDates: MemberOrgDate[],
  memberId: string,
  log: Logger,
): Promise<MemberOrgDate[]> {
  const knownOrgIds = new Set(existingOrgs.map((o) => o.organizationId))
  const orgIdsToVerify = [...new Set(orgDates.map((d) => d.organizationId))].filter(
    (id) => !knownOrgIds.has(id),
  )

  if (orgIdsToVerify.length === 0) {
    return orgDates
  }

  const existingOrgIds = new Set((await findOrgsByIds(qx, orgIdsToVerify)).map((o) => o.id))
  const missingOrgIds = orgIdsToVerify.filter((id) => !existingOrgIds.has(id))

  if (missingOrgIds.length === 0) {
    return orgDates
  }

  const mergedPrimaryIds = await findMergedPrimaryIds(qx, MergeActionType.ORG, missingOrgIds)
  const deletedOrgIds = new Set(missingOrgIds.filter((id) => !mergedPrimaryIds.has(id)))

  if (deletedOrgIds.size > 0) {
    log.warn(
      { memberId, deletedOrgIds: [...deletedOrgIds] },
      'Dropping queued stint dates for organizations that no longer exist.',
    )
  }

  return orgDates
    .filter((d) => !deletedOrgIds.has(d.organizationId))
    .map((d) =>
      mergedPrimaryIds.has(d.organizationId)
        ? { ...d, organizationId: mergedPrimaryIds.get(d.organizationId) }
        : d,
    )
}

/**
 * Purges a member from the queue and their associated Redis entries.
 */
async function purgeMember(redis: RedisClient, memberId: string): Promise<void> {
  const datesKey = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`

  await redis.multi().del(datesKey).sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId).exec()
}

/**
 * Applies the stint changes to the database.
 */
async function applyStintChanges(qx: QueryExecutor, changes: MemberOrgStintChange[]) {
  const insertOrgIds = [
    ...new Set(
      changes.filter((c) => c.type === 'insert').map((c) => c.organizationId),
    ),
  ]
  const orgAffiliationPolicies = await fetchManyOrganizationAffiliationPolicies(qx, insertOrgIds)

  for (const change of changes) {
    if (change.type === 'insert') {
      const memberOrganizationId = await createMemberOrganization(qx, change.memberId, {
        organizationId: change.organizationId,
        dateStart: change.dateStart,
        dateEnd: change.dateEnd,
        source: OrganizationSource.EMAIL_DOMAIN,
      })

      if (memberOrganizationId && orgAffiliationPolicies.get(change.organizationId)) {
        await changeMemberOrganizationAffiliationOverrides(qx, [
          {
            memberId: change.memberId,
            memberOrganizationId,
            allowAffiliation: false,
          },
        ])
        await deleteMemberSegmentAffiliations(qx, {
          memberId: change.memberId,
          organizationId: change.organizationId,
        })
      }
    } else {
      await updateMemberOrganization(qx, change.memberId, change.id, {
        dateStart: change.dateStart,
        dateEnd: change.dateEnd,
      })
    }
  }

  await deleteUndatedMemberOrganizations(qx, changes[0].memberId, [
    ...new Set(changes.map((c) => c.organizationId)),
  ])
}

export default job
