import commandLineArgs from 'command-line-args'

import { DB_CONFIG } from '@/conf'
import { getSearchSyncApiClient } from '@/utils/apiClients'
import {
  getAttributeValue,
  getCountry,
  hasAttributeValue,
  replaceDoubleQuotes,
  setAttributesDefaultValues,
} from '@crowd/common'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getPlatformPriorityArray } from '@crowd/data-access-layer/src/members/attributeSettings'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { IAttributes } from '@crowd/types'

const log = getServiceLogger()

const BATCH_SIZE = 500
const TEST_RUN_LIMIT = 10

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: `Stop after updating ${TEST_RUN_LIMIT} members.`,
  },
]

const parameters = commandLineArgs(options)

interface ICrustdataLocationRow {
  memberId: string
  location: string | null
  country: string | null
  locationAttribute: Record<string, string> | null
  countryAttribute: Record<string, string> | null
}

// Returns every scanned cache row so the caller can page by memberId. Location and country are
// only set when Crustdata is the member's sole source with a single verified profile.
async function fetchCrustdataLocationRows(
  qx: QueryExecutor,
  limit: number,
  afterMemberId?: string,
): Promise<ICrustdataLocationRow[]> {
  return qx.select(
    `
      WITH batch AS (
        SELECT "memberId", data
        FROM "memberEnrichmentCache"
        WHERE source = 'crustdata'
          AND data IS NOT NULL
          ${afterMemberId ? `AND "memberId" > $(afterMemberId)` : ''}
        ORDER BY "memberId"
        LIMIT $(limit)
      ),
      verified_profiles AS (
        SELECT b."memberId", (array_agg(p))[1] -> 'basic_profile' -> 'location' AS location
        FROM batch b
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE WHEN jsonb_typeof(b.data) = 'array' THEN b.data ELSE '[]'::jsonb END
        ) p
        WHERE (p -> 'metadata' ->> 'isFromVerifiedSource')::boolean
        GROUP BY b."memberId"
        HAVING count(*) = 1
      )
      SELECT
        b."memberId",
        v.location ->> 'raw' AS location,
        v.location ->> 'country' AS country,
        m.attributes -> 'location' AS "locationAttribute",
        m.attributes -> 'country' AS "countryAttribute"
      FROM batch b
      LEFT JOIN verified_profiles v
        ON v."memberId" = b."memberId"
        AND NOT EXISTS (
          SELECT 1
          FROM "memberEnrichmentCache" o
          WHERE o."memberId" = b."memberId"
            AND o.source <> 'crustdata'
            AND o.data IS NOT NULL
        )
      LEFT JOIN members m ON m.id = v."memberId"
      ORDER BY b."memberId"
    `,
    { limit, afterMemberId },
  )
}

async function mergeLocationAttributes(
  qx: QueryExecutor,
  memberId: string,
  changed: IAttributes,
): Promise<void> {
  await qx.result(
    `
      UPDATE members
      SET attributes = attributes || $(changed)::jsonb,
          "updatedAt" = NOW()
      WHERE id = $(memberId)
    `,
    { memberId, changed: JSON.stringify(changed) },
  )
}

async function buildLocationAttributes(
  row: ICrustdataLocationRow,
  priorities: string[],
): Promise<IAttributes | null> {
  const changed: IAttributes = {}

  if (row.location && !row.locationAttribute?.enrichment) {
    changed.location = { ...row.locationAttribute, enrichment: replaceDoubleQuotes(row.location) }
  }

  if (row.country && !row.countryAttribute?.enrichment) {
    changed.country = { ...row.countryAttribute, enrichment: replaceDoubleQuotes(row.country) }
  }

  if (Object.keys(changed).length === 0) {
    return null
  }

  const country = changed.country ?? row.countryAttribute
  if (!hasAttributeValue(country)) {
    const inferredCountry = getCountry(getAttributeValue(changed.location ?? row.locationAttribute))
    if (inferredCountry) {
      changed.country = { ...country, system: inferredCountry }
    }
  }

  return (await setAttributesDefaultValues(changed, priorities)) as IAttributes
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false

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

  log.info({ testRun }, 'Running script with the following parameters!')

  let updatedCount = 0
  let afterMemberId: string | undefined
  const failedSyncMemberIds: string[] = []
  let rows: ICrustdataLocationRow[]

  do {
    rows = await fetchCrustdataLocationRows(qx, BATCH_SIZE, afterMemberId)
    let candidates = rows.filter((row) => row.location || row.country)

    if (testRun) {
      candidates = candidates.slice(0, TEST_RUN_LIMIT - updatedCount)
    }

    for (const chunk of chunkArray(candidates, 20)) {
      await Promise.all(
        chunk.map(async (row) => {
          const changed = await buildLocationAttributes(row, priorities)
          if (!changed) {
            return
          }

          await mergeLocationAttributes(qx, row.memberId, changed)
          updatedCount++

          if (testRun) {
            log.info(
              {
                memberId: row.memberId,
                before: { location: row.locationAttribute, country: row.countryAttribute },
                after: changed,
              },
              'Updated member!',
            )
          }

          // The attributes are already written, so a rerun would skip this member and never sync it.
          try {
            await searchSyncApi.triggerMemberSync(row.memberId)
          } catch (err) {
            log.error({ memberId: row.memberId, err }, 'Failed to sync member to OpenSearch!')
            failedSyncMemberIds.push(row.memberId)
          }
        }),
      )
    }

    afterMemberId = rows[rows.length - 1]?.memberId

    log.info({ scanned: rows.length, updatedCount }, 'Batch processed!')
  } while (rows.length === BATCH_SIZE && !(testRun && updatedCount >= TEST_RUN_LIMIT))

  log.info({ updatedCount, failedSyncMemberIds }, 'Backfill finished!')

  process.exit(0)
})
