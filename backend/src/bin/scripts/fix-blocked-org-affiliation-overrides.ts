import commandLineArgs from 'command-line-args'

import { signalMemberUpdate } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { changeMemberOrganizationAffiliationOverrides } from '@crowd/data-access-layer/src/member-organization-affiliation'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 member organizations per batch, single batch).',
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
  try {
    const testRun = parameters.testRun ?? false
    const BATCH_SIZE = testRun ? 10 : 100

    const db = await getDbConnection({
      host: DB_CONFIG.writeHost,
      port: DB_CONFIG.port,
      database: DB_CONFIG.database,
      user: DB_CONFIG.username,
      password: DB_CONFIG.password,
    })

    const qx = pgpQx(db)
    const temporal = await getTemporalClient(TEMPORAL_CONFIG)

    log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!')

    const refreshByMemberId = new Map<string, Set<string>>()
    let memberOrgs = []

    do {
      memberOrgs = await qx.select(
        `
          SELECT
            mo.id AS "memberOrganizationId",
            mo."memberId",
            mo."organizationId"
          FROM "memberOrganizations" mo
          JOIN organizations o ON o.id = mo."organizationId"
          LEFT JOIN "memberOrganizationAffiliationOverrides" moao
            ON moao."memberOrganizationId" = mo.id
          WHERE mo."deletedAt" IS NULL
            AND o."isAffiliationBlocked" IS TRUE
            AND (moao.id IS NULL OR moao."allowAffiliation" IS TRUE)
          ORDER BY mo.id
          LIMIT $(limit)
        `,
        { limit: BATCH_SIZE },
      )

      if (memberOrgs.length === 0) {
        break
      }

      await changeMemberOrganizationAffiliationOverrides(
        qx,
        memberOrgs.map((mo) => ({
          memberId: mo.memberId,
          memberOrganizationId: mo.memberOrganizationId,
          allowAffiliation: false,
        })),
      )

      for (const { memberId, organizationId } of memberOrgs) {
        if (!refreshByMemberId.has(memberId)) {
          refreshByMemberId.set(memberId, new Set())
        }

        refreshByMemberId.get(memberId)!.add(organizationId)
      }

      if (testRun) {
        log.info('Test run - stopping after first batch!')
        break
      }
    } while (memberOrgs.length > 0)

    for (const [memberId, organizationIds] of refreshByMemberId) {
      await signalMemberUpdate(temporal, memberId, {
        memberOrganizationIds: [...organizationIds],
      })
    }

    process.exit(0)
  } catch (error) {
    log.error(error, 'Script crashed due to a fatal error')
    process.exit(1)
  }
})
