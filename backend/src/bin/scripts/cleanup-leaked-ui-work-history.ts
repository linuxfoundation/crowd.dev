import commandLineArgs from 'command-line-args'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'
import { signalMemberUpdate } from '@crowd/common_services'
import { QueryExecutor, deleteMemberOrganizations, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'
import { OrganizationSource } from '@crowd/types'

const log = getServiceLogger()

const MIN_MEMBERS_SHARING_ROLE = 10
const MIN_LEAKED_ROLES_PER_MEMBER = 6
const TEST_RUN_LIMIT = 10

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Clean only a few members to verify the script before a full run.',
  },
]

const parameters = commandLineArgs(options)

async function findMembersWithLeakedRoles(
  qx: QueryExecutor,
  limit: number | null,
): Promise<{ memberId: string; roleIds: string[] }[]> {
  return qx.select(
    `
      WITH shared_roles AS MATERIALIZED (
        SELECT "organizationId", title, "dateStart", "dateEnd"
        FROM "memberOrganizations"
        WHERE source = $(source) AND "deletedAt" IS NULL
        GROUP BY 1, 2, 3, 4
        HAVING count(*) >= $(minMembersSharingRole)
      )
      SELECT mo."memberId", array_agg(mo.id) AS "roleIds"
      FROM "memberOrganizations" mo
      JOIN shared_roles s
        ON s."organizationId" = mo."organizationId"
        AND s.title IS NOT DISTINCT FROM mo.title
        AND s."dateStart" IS NOT DISTINCT FROM mo."dateStart"
        AND s."dateEnd" IS NOT DISTINCT FROM mo."dateEnd"
      WHERE mo.source = $(source) AND mo."deletedAt" IS NULL
      GROUP BY mo."memberId"
      HAVING count(*) >= $(minLeakedRolesPerMember)
      LIMIT $(limit)
    `,
    {
      source: OrganizationSource.UI,
      minMembersSharingRole: MIN_MEMBERS_SHARING_ROLE,
      minLeakedRolesPerMember: MIN_LEAKED_ROLES_PER_MEMBER,
      limit,
    },
  )
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
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  log.info({ testRun }, 'Running script with the following parameters!')

  const members = await findMembersWithLeakedRoles(qx, testRun ? TEST_RUN_LIMIT : null)

  log.info({ members: members.length }, 'Found members with leaked ui roles!')

  for (const { memberId, roleIds } of members) {
    await deleteMemberOrganizations(qx, memberId, { ids: roleIds, softDelete: false })
    await signalMemberUpdate(temporal, memberId, { syncToOpensearch: true })

    log.info({ memberId, roles: roleIds.length }, 'Removed leaked roles!')
  }

  log.info({ members: members.length }, 'Cleanup finished!')

  process.exit(0)
})
