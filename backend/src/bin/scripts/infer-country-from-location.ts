import commandLineArgs from 'command-line-args'

import { DB_CONFIG } from '@/conf'
import { IS_LLM_ENABLED } from '@crowd/common'
import { LlmService, inferCountries } from '@crowd/common_services'
import {
  ICountryCandidate,
  QueryExecutor,
  pgpQx,
  saveMemberCountries,
  saveOrganizationCountries,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const BATCH_SIZE = 1000
const TEST_RUN_LIMIT = 100

const options = [
  {
    name: 'type',
    type: String,
    description: 'Entity to backfill: member or org.',
  },
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: `Process a single batch of ${TEST_RUN_LIMIT} candidates and exit.`,
  },
]

const parameters = commandLineArgs(options)

async function fetchMembers(
  qx: QueryExecutor,
  limit: number,
  afterId?: string,
): Promise<ICountryCandidate[]> {
  return qx.select(
    `
      select id, attributes -> 'location' ->> 'default' as location
      from members
      where "deletedAt" is null
        ${afterId ? 'and id > $(afterId)' : ''}
        and nullif(trim(attributes -> 'location' ->> 'default'), '') is not null
        and not exists (
          select 1
          from jsonb_each_text(coalesce(attributes -> 'country', '{}'::jsonb)) c
          where nullif(trim(c.value), '') is not null
        )
      order by id
      limit $(limit)
    `,
    { limit, afterId },
  )
}

async function fetchOrganizations(
  qx: QueryExecutor,
  limit: number,
  afterId?: string,
): Promise<ICountryCandidate[]> {
  return qx.select(
    `
      select o.id, o.location
      from organizations o
      where o."deletedAt" is null
        ${afterId ? 'and o.id > $(afterId)' : ''}
        and nullif(trim(o.location), '') is not null
        and nullif(trim(o.country), '') is null
        and not exists (
          select 1
          from "orgAttributes" a
          where a."organizationId" = o.id
            and a.name = 'country'
            and nullif(trim(a.value), '') is not null
        )
      order by o.id
      limit $(limit)
    `,
    { limit, afterId },
  )
}

setImmediate(async () => {
  const { type } = parameters
  if (type !== 'member' && type !== 'org') {
    log.error({ type }, 'Invalid --type! Use member or org.')
    process.exit(1)
  }

  const bedrockCredentials = {
    accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
    secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
  }

  if (!IS_LLM_ENABLED || !bedrockCredentials.accessKeyId || !bedrockCredentials.secretAccessKey) {
    log.error('LLM is disabled! Set CROWD_ENABLE_LLM and the Bedrock credentials.')
    process.exit(1)
  }

  const isMember = type === 'member'
  const fetchCandidates = isMember ? fetchMembers : fetchOrganizations
  const saveCountries = isMember ? saveMemberCountries : saveOrganizationCountries

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
  const llm = new LlmService(qx, bedrockCredentials, log)

  log.info({ type, testRun }, 'Running script with the following parameters!')

  let processedCount = 0
  let updatedCount = 0
  let afterId: string | undefined
  let candidates: ICountryCandidate[]

  do {
    candidates = await fetchCandidates(qx, limit, afterId)

    const updates = await inferCountries(qx, llm, candidates, { withHints: isMember })
    await saveCountries(qx, updates)

    if (testRun) {
      const locations = new Map(candidates.map((c) => [c.id, c.location]))
      updates.forEach((u) => log.info({ ...u, location: locations.get(u.id) }, 'Updated country!'))
    }

    processedCount += candidates.length
    updatedCount += updates.length
    afterId = candidates[candidates.length - 1]?.id

    log.info({ processedCount, updatedCount, afterId }, 'Batch processed!')
  } while (candidates.length === limit && !testRun)

  log.info({ type, processedCount, updatedCount }, 'Backfill finished!')

  process.exit(0)
})
