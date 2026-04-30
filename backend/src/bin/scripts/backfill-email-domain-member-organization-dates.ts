import commandLineArgs from 'command-line-args'

import { inferMemberOrganizationStintChanges } from '@crowd/common_services'
import {
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicy,
  createMemberOrganization,
  fetchEmailDomainMemberOrganizationActivityDates,
  fetchEmailDomainMemberOrganizationsWithoutDates,
  fetchMemberOrganizationsBySource,
  pgpQx,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { getRedisClient } from '@crowd/redis'
import { OrganizationSource } from '@crowd/types'

import { DB_CONFIG, REDIS_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 1 batch and 10 members).',
  },
  {
    name: 'afterMemberId',
    alias: 'a',
    type: String,
    description: 'The member ID to start processing after.',
  },
  {
    name: 'batchSize',
    alias: 'b',
    type: Number,
    description: 'The number of members to fetch in each batch.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = parameters.batchSize ?? (testRun ? 10 : 500)
  let afterMemberId = parameters.afterMemberId ?? undefined

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const redis = await getRedisClient(REDIS_CONFIG, true)

  log.info({ testRun, BATCH_SIZE, afterMemberId }, 'Running script with the following parameters!')

  let hasMore = true

  while (hasMore) {
    const memberIds = await fetchEmailDomainMemberOrganizationsWithoutDates(
      qx,
      BATCH_SIZE,
      afterMemberId,
    )

    if (memberIds.length > 0) {
      for (const chunk of chunkArray(memberIds, 50)) {
        await Promise.all(
          chunk.map(async (memberId) => {
            if (testRun) {
              log.info({ memberId }, 'Processing member!')
            }

            try {
              const [existingMemberOrganizations, activityDates] = await Promise.all([
                fetchMemberOrganizationsBySource(qx, memberId, OrganizationSource.EMAIL_DOMAIN),
                fetchEmailDomainMemberOrganizationActivityDates(qx, memberId),
              ])

              const changes = inferMemberOrganizationStintChanges(
                memberId,
                existingMemberOrganizations,
                activityDates,
              )

              if (testRun) {
                log.info(
                  { existingMemberOrganizations, activityDates, changes },
                  'Previewing changes for member.',
                )
              }

              if (changes.length > 0) {
                await qx.tx(async (tx) => {
                  for (const change of changes) {
                    if (change.type === 'insert') {
                      const memberOrganizationId = await createMemberOrganization(tx, memberId, {
                        organizationId: change.organizationId,
                        dateStart: change.dateStart,
                        dateEnd: change.dateEnd,
                        source: OrganizationSource.EMAIL_DOMAIN,
                      })

                      const isAffiliationBlocked = await checkOrganizationAffiliationPolicy(
                        tx,
                        change.organizationId,
                      )

                      if (memberOrganizationId && isAffiliationBlocked) {
                        await changeMemberOrganizationAffiliationOverrides(tx, [
                          {
                            memberId,
                            memberOrganizationId,
                            allowAffiliation: false,
                          },
                        ])
                      }
                    } else if (change.type === 'update') {
                      await updateMemberOrganization(tx, memberId, change.id, {
                        dateStart: change.dateStart,
                        dateEnd: change.dateEnd,
                      })
                    }

                    if (testRun) {
                      log.info(
                        { memberId, orgId: change.organizationId, type: change.type },
                        'Member organization updated.',
                      )
                    }
                  }
                })
                await redis.sAdd('recalculate-member-affiliations', [memberId])
              } else if (testRun) {
                log.info({ memberId }, 'No changes found for member!')
              }
            } catch (err) {
              log.error({ memberId, err }, 'Failed to process for member!')
              throw err
            }
          }),
        )
      }

      const lastMemberId = memberIds[memberIds.length - 1]
      afterMemberId = lastMemberId

      log.info({ lastMemberId, count: memberIds.length }, 'Batch processed!')

      if (testRun || memberIds.length < BATCH_SIZE) {
        hasMore = false
      }
    } else {
      hasMore = false
    }
  }

  process.exit(0)
})
