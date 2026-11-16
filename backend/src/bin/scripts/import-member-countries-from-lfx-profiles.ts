import commandLineArgs from 'command-line-args'
import isEqual from 'lodash/isEqual'

import { DB_CONFIG } from '@/conf'
import { getSearchSyncApiClient } from '@/utils/apiClients'
import { setAttributesDefaultValues } from '@crowd/common'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getPlatformPriorityArray } from '@crowd/data-access-layer/src/members/attributeSettings'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { SnowflakeClient } from '@crowd/snowflake'
import { MemberAttributeName, PlatformType } from '@crowd/types'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 LFX profiles).',
  },
]

const parameters = commandLineArgs(options)

interface IMemberLfid {
  memberId: string
  lfid: string
  country: Record<string, string> | null
}

async function fetchProfileCountries(): Promise<Map<string, string>> {
  const snowflake = SnowflakeClient.fromEnv({ parentLog: log })

  try {
    const rows = await snowflake.run<{ lfid: string; country: string }>(`
      SELECT LOWER(u.username) AS "lfid", TRIM(m.value::string) AS "country"
      FROM fivetran_ingest.auth0.users u
      JOIN fivetran_ingest.auth0.user_metadata m ON m.users_id = u.id
      WHERE m.name = 'country'
        AND NOT m._fivetran_deleted
        AND NOT u._fivetran_deleted
        AND u.username IS NOT NULL
        AND NULLIF(TRIM(m.value::string), '') IS NOT NULL
    `)

    return new Map(rows.map(({ lfid, country }) => [lfid, country]))
  } finally {
    await snowflake.destroy()
  }
}

async function findMembersByLfids(qx: QueryExecutor, lfids: string[]): Promise<IMemberLfid[]> {
  return qx.select(
    `
      SELECT mi."memberId", LOWER(mi.value) AS lfid, m.attributes -> 'country' AS country
      FROM "memberIdentities" mi
      JOIN members m ON m.id = mi."memberId" AND m."deletedAt" IS NULL
      WHERE mi.platform = 'lfid'
        AND mi.type = 'username'
        AND mi.verified
        AND mi."deletedAt" IS NULL
        AND LOWER(mi.value) = ANY($(lfids)::text[])
      ORDER BY lfid
    `,
    { lfids },
  )
}

async function updateMemberCountry(
  qx: QueryExecutor,
  memberId: string,
  country: Record<string, string>,
): Promise<void> {
  await qx.result(
    `
      UPDATE members
      SET attributes = attributes || jsonb_build_object('country', $(country)::jsonb),
          "updatedAt" = NOW()
      WHERE id = $(memberId)
    `,
    { memberId, country: JSON.stringify(country) },
  )
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = testRun ? 10 : 1000

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const searchSyncApi = await getSearchSyncApiClient()
  const priorities = await getPlatformPriorityArray(qx)

  log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!')

  const profileCountries = await fetchProfileCountries()
  log.info({ profiles: profileCountries.size }, 'Fetched LFX profile countries!')

  let updatedCount = 0

  for (const lfids of chunkArray([...profileCountries.keys()], BATCH_SIZE)) {
    const members = await findMembersByLfids(qx, lfids)
    const seenMemberIds = new Set<string>()

    for (const { memberId, lfid, country: current } of members) {
      if (seenMemberIds.has(memberId)) {
        continue
      }
      seenMemberIds.add(memberId)

      const { country } = (await setAttributesDefaultValues(
        {
          [MemberAttributeName.COUNTRY]: {
            ...current,
            [PlatformType.LFID]: profileCountries.get(lfid),
          },
        },
        priorities,
      )) as { country?: Record<string, string> }

      if (!country || isEqual(country, current)) {
        continue
      }

      await updateMemberCountry(qx, memberId, country)
      updatedCount++

      if (testRun) {
        log.info({ memberId, before: current, after: country }, 'Updated member country!')
      }

      try {
        await searchSyncApi.triggerMemberSync(memberId)
      } catch (err) {
        log.error({ memberId, err }, 'Failed to sync member to OpenSearch!')
      }
    }

    log.info({ updatedCount }, 'Batch processed!')

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  }

  process.exit(0)
})
