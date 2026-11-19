import commandLineArgs from 'command-line-args'
import { isEqual } from 'lodash'

import { DB_CONFIG } from '@/conf'
import { normalizeGeoAttributes, timeout, toGeo } from '@crowd/common'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const BATCH_SIZE = 1000
const TEST_RUN_LIMIT = 100
const BATCH_DELAY_MS = 1000

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
    description: `Process a single batch of ${TEST_RUN_LIMIT} rows and exit.`,
  },
]

const parameters = commandLineArgs(options)

interface IMemberCountry {
  id: string
  country: Record<string, unknown>
  geo: Record<string, unknown> | null
}

interface IOrganizationCountry {
  organizationId: string
  source: string
  default: boolean
  value: string
}

async function backfillMembers(qx: QueryExecutor, limit: number, afterId?: string) {
  const members: IMemberCountry[] = await qx.select(
    `
      select id, attributes -> 'country' as country, attributes -> 'geo' as geo
      from members
      where "deletedAt" is null
        ${afterId ? 'and id > $(afterId)' : ''}
        and attributes ? 'country'
      order by id
      limit $(limit)
    `,
    { limit, afterId },
  )

  const updates = members.flatMap(({ id, country, geo }) => {
    const derived = normalizeGeoAttributes(country, geo ?? {}).geo
    return Object.keys(derived).length > 0 && !isEqual(derived, geo)
      ? [{ id, country, geo: derived }]
      : []
  })

  if (updates.length > 0) {
    await qx.result(
      `
        update members m
        set attributes = jsonb_set(m.attributes, '{geo}', u.geo)
        from unnest($(ids)::uuid[], $(countries)::jsonb[], $(geos)::jsonb[]) as u(id, country, geo)
        where m.id = u.id
          and m.attributes -> 'country' = u.country
      `,
      {
        ids: updates.map((u) => u.id),
        countries: updates.map((u) => JSON.stringify(u.country)),
        geos: updates.map((u) => JSON.stringify(u.geo)),
      },
    )
  }

  return {
    processed: members.length,
    updated: updates.length,
    lastId: members[members.length - 1]?.id,
  }
}

async function backfillOrganizations(qx: QueryExecutor, limit: number, afterId?: string) {
  const organizations: { id: string }[] = await qx.select(
    `
      select id
      from organizations
      where "deletedAt" is null
        ${afterId ? 'and id > $(afterId)' : ''}
      order by id
      limit $(limit)
    `,
    { limit, afterId },
  )

  const ids = organizations.map((o) => o.id)
  const countries: IOrganizationCountry[] = ids.length
    ? await qx.select(
        `
          select a."organizationId", a.source, a."default", a.value
          from "orgAttributes" a
          where a."organizationId" in ($(ids:csv))
            and a.name = 'country'
            and not exists (
              select 1
              from "orgAttributes" g
              where g."organizationId" = a."organizationId"
                and g.name = 'geo'
            )
        `,
        { ids },
      )
    : []

  const geos = countries
    .map((c) => ({ ...c, geo: toGeo(c.value) }))
    .filter((c) => c.geo)
    .map((c) => ({ ...c, value: JSON.stringify(c.geo) }))
  const defaults = geos.filter((g) => g.default)

  if (geos.length > 0) {
    await qx.tx(async (tx) => {
      await tx.result(
        `
          insert into "orgAttributes" ("organizationId", name, source, "default", value)
          select id, 'geo', source, "default", value
          from unnest($(ids)::uuid[], $(sources)::text[], $(defaults)::boolean[], $(values)::text[])
            as u(id, source, "default", value)
          on conflict ("organizationId", name, source, md5(value)) do nothing
        `,
        {
          ids: geos.map((g) => g.organizationId),
          sources: geos.map((g) => g.source),
          defaults: geos.map((g) => g.default),
          values: geos.map((g) => g.value),
        },
      )

      if (defaults.length > 0) {
        await tx.result(
          `
            update organizations o
            set geo = u.geo::jsonb
            from unnest($(ids)::uuid[], $(geos)::text[]) as u(id, geo)
            where o.id = u.id
          `,
          {
            ids: defaults.map((g) => g.organizationId),
            geos: defaults.map((g) => g.value),
          },
        )
      }
    })
  }

  return { processed: organizations.length, updated: geos.length, lastId: ids[ids.length - 1] }
}

setImmediate(async () => {
  const { type } = parameters
  if (type !== 'member' && type !== 'org') {
    log.error({ type }, 'Invalid --type! Use member or org.')
    process.exit(1)
  }

  const backfill = type === 'member' ? backfillMembers : backfillOrganizations
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

  log.info({ type, testRun }, 'Running script with the following parameters!')

  let processedCount = 0
  let updatedCount = 0
  let afterId: string | undefined
  let processed: number

  do {
    const batch = await backfill(qx, limit, afterId)
    processed = batch.processed
    processedCount += batch.processed
    updatedCount += batch.updated
    afterId = batch.lastId

    log.info({ processedCount, updatedCount, afterId }, 'Batch processed!')
    await timeout(BATCH_DELAY_MS)
  } while (processed === limit && !testRun)

  log.info({ type, processedCount, updatedCount }, 'Backfill finished!')

  process.exit(0)
})
