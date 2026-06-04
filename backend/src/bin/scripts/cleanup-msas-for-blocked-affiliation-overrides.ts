import commandLineArgs from 'command-line-args'

import { signalMemberUpdate } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Dry run — log counts and member ids only, no writes.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

const MSAS_TO_CLEAN_SQL = `
  SELECT DISTINCT
    msa.id,
    msa."memberId",
    mo."organizationId"
  FROM "memberOrganizationAffiliationOverrides" moao
  JOIN "memberOrganizations" mo ON mo.id = moao."memberOrganizationId"
  JOIN "memberSegmentAffiliations" msa
    ON msa."memberId" = moao."memberId"
    AND msa."organizationId" = mo."organizationId"
    AND msa."deletedAt" IS NULL
  WHERE moao."allowAffiliation" = false
`

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
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  log.info({ testRun }, 'Running cleanup-msas-for-blocked-affiliation-overrides')

  const rows: { id: string; memberId: string; organizationId: string }[] = await qx.select(
    MSAS_TO_CLEAN_SQL,
  )

  const memberIds = [...new Set(rows.map((r) => r.memberId))]

  log.info(
    { msaCount: rows.length, memberCount: memberIds.length, memberIds },
    'MSAs to soft-delete and members to recalculate',
  )

  if (rows.length === 0) {
    process.exit(0)
  }

  if (testRun) {
    log.info('Test run — no changes applied')
    process.exit(0)
  }

  const msaIds = rows.map((r) => r.id)

  await qx.result(
    `
      UPDATE "memberSegmentAffiliations"
      SET "deletedAt" = NOW()
      WHERE id IN ($(msaIds:csv))
        AND "deletedAt" IS NULL
    `,
    { msaIds },
  )

  log.info({ msaCount: msaIds.length }, 'Soft-deleted MSAs')

  for (const memberId of memberIds) {
    await signalMemberUpdate(temporal, memberId)
    log.info({ memberId }, 'Triggered memberUpdate')
  }

  log.info({ memberCount: memberIds.length }, 'Done')
  process.exit(0)
})
