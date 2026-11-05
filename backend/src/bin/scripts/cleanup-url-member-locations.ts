import commandLineArgs from 'command-line-args'

import { DB_CONFIG } from '@/conf'
import { omitUrlValues, setAttributesDefaultValues } from '@crowd/common'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getPlatformPriorityArray } from '@crowd/data-access-layer/src/members/attributeSettings'
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
    description: `Stop after cleaning ${TEST_RUN_LIMIT} members.`,
  },
]

const parameters = commandLineArgs(options)

interface IMemberLocation {
  id: string
  location: Record<string, unknown>
}

async function fetchMembersWithUrlLocation(
  qx: QueryExecutor,
  limit: number,
  afterId?: string,
): Promise<IMemberLocation[]> {
  return qx.select(
    `
      SELECT id, attributes -> 'location' AS location
      FROM members
      WHERE "deletedAt" IS NULL
        AND jsonb_path_exists(attributes -> 'location', '$.* ? (@ like_regex "^[[:space:]]*https?://" flag "i")')
        ${afterId ? 'AND id > $(afterId)' : ''}
      ORDER BY id
      LIMIT $(limit)
    `,
    { limit, afterId },
  )
}

async function updateLocation(
  qx: QueryExecutor,
  memberId: string,
  attributes: IAttributes,
): Promise<void> {
  await qx.result(
    `
      UPDATE members
      SET attributes = (attributes - 'location') || $(attributes)::jsonb,
          "updatedAt" = NOW()
      WHERE id = $(memberId)
    `,
    { memberId, attributes: JSON.stringify(attributes) },
  )
}

async function cleanLocation(
  location: Record<string, unknown>,
  priorities: string[],
): Promise<IAttributes> {
  return (await setAttributesDefaultValues(
    { location: omitUrlValues(location) },
    priorities,
  )) as IAttributes
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const limit = testRun ? TEST_RUN_LIMIT : BATCH_SIZE

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const priorities = await getPlatformPriorityArray(qx)

  log.info({ testRun }, 'Running script with the following parameters!')

  let cleanedCount = 0
  let afterId: string | undefined
  let members: IMemberLocation[]

  do {
    members = await fetchMembersWithUrlLocation(qx, limit, afterId)

    for (const member of members) {
      const attributes = await cleanLocation(member.location, priorities)
      await updateLocation(qx, member.id, attributes)
      cleanedCount++

      if (testRun) {
        log.info(
          { memberId: member.id, before: member.location, after: attributes.location ?? null },
          'Cleaned member location!',
        )
      }
    }

    afterId = members[members.length - 1]?.id

    log.info({ batchSize: members.length, cleanedCount, afterId }, 'Batch processed!')
  } while (members.length === limit && !testRun)

  log.info({ cleanedCount }, 'Cleanup finished!')

  process.exit(0)
})
