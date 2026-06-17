import commandLineArgs from 'command-line-args'

import {
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
} from '@crowd/common_services'
import { deleteMemberOrganizations, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { getRedisClient } from '@crowd/redis'

import { DB_CONFIG, REDIS_CONFIG } from '@/conf'

type RedisClient = Awaited<ReturnType<typeof getRedisClient>>

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 members).',
  },
  {
    name: 'memberIds',
    alias: 'm',
    type: String,
    multiple: true,
    description: 'Specific member IDs to process.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

async function getBotMembersWithOrgOrActivityRelations(
  qx: QueryExecutor,
  limit?: number,
): Promise<string[]> {
  const rows = await qx.select(
    `
    SELECT DISTINCT m.id
    FROM members m
    WHERE COALESCE((m.attributes -> 'isBot' ->> 'default')::boolean, false) = true
      AND m."deletedAt" IS NULL
      AND (
        EXISTS (
          SELECT 1
          FROM "memberOrganizations" mo
          WHERE mo."memberId" = m.id
            AND mo."deletedAt" IS NULL
        )
        OR EXISTS (
          SELECT 1
          FROM "activityRelations" ar
          WHERE ar."memberId" = m.id
            AND ar."organizationId" IS NOT NULL
        )
      )
    ORDER BY m.id
    ${limit ? 'LIMIT $(limit)' : ''}
    `,
    limit ? { limit } : {},
  )

  return rows.map((row: { id: string }) => row.id)
}

async function updateMemberActivityRelations(
  qx: QueryExecutor,
  memberId: string,
  batchSize = 5000,
): Promise<void> {
  let rowsUpdated

  do {
    rowsUpdated = await qx.result(
      `
      UPDATE "activityRelations"
      SET "organizationId" = NULL, "updatedAt" = CURRENT_TIMESTAMP
      WHERE "memberId" = $(memberId)
        AND "organizationId" IS NOT NULL
        AND "activityId" IN (
          SELECT ar."activityId"
          FROM "activityRelations" ar
          WHERE ar."memberId" = $(memberId)
            AND ar."organizationId" IS NOT NULL
          LIMIT $(batchSize)
        )
      `,
      { memberId, batchSize },
    )
  } while (rowsUpdated === batchSize)
}


async function processMember(qx: QueryExecutor, redis: RedisClient, memberId: string): Promise<void> {
  log.info({ memberId }, 'Processing member')

  // Clear stint inference Redis data
  const datesKey = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`
  await redis.multi().del(datesKey).sRem(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId).exec()

  // Delete member organizations
  await deleteMemberOrganizations(qx, memberId, undefined, false)

  // Update member activity relations
  await updateMemberActivityRelations(qx, memberId)
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const memberIds: string[] | undefined = parameters.memberIds

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const redis = await getRedisClient(REDIS_CONFIG, true)

  log.info({ testRun, memberIds }, 'Running script with the following parameters!')

  const impactedMemberIds =
    memberIds?.length > 0 ? memberIds : await getBotMembersWithOrgOrActivityRelations(qx, testRun ? 10 : undefined)

  log.info({ totalMembers: impactedMemberIds.length }, 'Found members to process')

  if (impactedMemberIds.length === 0) {
    process.exit(0)
  }

  const chunks = chunkArray(impactedMemberIds, 10)

  for (const chunk of chunks) {
    await Promise.all(
      chunk.map(async (memberId: string) => {
        try {
          await processMember(qx, redis, memberId)
        } catch (err) {
          log.error({ memberId, err }, 'Failed to process member!')
        }
      }),
    )
  }

  process.exit(0)
})
