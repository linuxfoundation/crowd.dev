import commandLineArgs from 'command-line-args'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { WorkflowIdConflictPolicy, getTemporalClient } from '@crowd/temporal'
import { TemporalWorkflowId } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'organizationId',
    alias: 'o',
    typeLabel: '{underline organizationId}',
    type: String,
    description: 'The organization ID to process members for.',
  },
  {
    name: 'dryRun',
    alias: 'd',
    type: Boolean,
    description: 'Run in dry-run mode (show what would be processed).',
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
  const organizationId = parameters.organizationId
  const dryRun = parameters.dryRun ?? false

  log.info({ organizationId, dryRun }, 'Running script with the following parameters!')

  const db = await getDbConnection({
    host: DB_CONFIG.readHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  try {
    const memberIds = await db.any(
      `
      SELECT DISTINCT ar."memberId" AS id
      FROM "activityRelations" ar
      JOIN "memberOrganizations" mo
        ON ar."memberId" = mo."memberId"
        AND ar."organizationId" = mo."organizationId"
      LEFT JOIN "memberOrganizationAffiliationOverrides" moao
        ON mo."id" = moao."memberOrganizationId"
      WHERE ar."organizationId" = $1
        AND (
          (
            mo."deletedAt" IS NOT NULL
            AND NOT EXISTS (
              SELECT 1
              FROM "memberOrganizations" mo2
              WHERE mo2."memberId" = mo."memberId"
                AND mo2."organizationId" = mo."organizationId"
                AND mo2."deletedAt" IS NULL
            )
          )
          OR (
            mo."deletedAt" IS NULL
            AND moao."allowAffiliation" = false
          )
        );
      `,
      [organizationId],
    )

    log.info(`Found ${memberIds.length} members to process`)

    if (memberIds.length === 0) {
      log.info('No members found. Implement the query to get actual memberIds.')
      return
    }

    if (dryRun) {
      log.info('DRY RUN - Would update affiliations for the following members:')
      memberIds.forEach((member: { id: string }) => {
        log.info(`  - Member ID: ${member.id}`)
      })
      return
    }

    let processedCount = 0
    for (const member of memberIds) {
      try {
        log.info(`Processing member: ${member.id}`)

        await temporal.workflow.signalWithStart('memberUpdate', {
          taskQueue: 'profiles',
          workflowId: `${TemporalWorkflowId.MEMBER_UPDATE}/${member.id}`,
          workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
          signal: 'refreshAffiliations',
          signalArgs: [
            {
              member: { id: member.id },
              memberOrganizationIds: [organizationId],
              syncToOpensearch: false,
            },
          ],
          retry: {
            maximumAttempts: 10,
          },
          args: [],
          searchAttributes: {
            TenantId: [DEFAULT_TENANT_ID],
          },
        })

        processedCount++
        log.info(`Successfully triggered workflow for member: ${member.id}`)
      } catch (error) {
        log.error(`Failed to process member ${member.id}:`, error)
      }
    }

    log.info(`Script completed. Processed ${processedCount}/${memberIds.length} members.`)
  } catch (error) {
    log.error('Script failed:', error)
    throw error
  }

  process.exit(0)
})
