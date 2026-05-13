import commandLineArgs from 'command-line-args'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { MemberField, findMemberById, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'execute',
    alias: 'e',
    type: Boolean,
    description: 'Actually execute the retries (without this, only logs what would be done)',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

interface StuckMerge {
  primaryId: string
  secondaryId: string
  createdAt: string
}

async function findStuckMerges(qx: any): Promise<StuckMerge[]> {
  // Find all merge actions stuck in merge-async-started from the incident period
  // These were started before the 2-hour timeout fix was deployed
  const stuckMerges = await qx.select(
    `
      SELECT "primaryId", "secondaryId", "createdAt"
      FROM "mergeActions"
      WHERE state = 'in-progress'
        AND step = 'merge-async-started'
        AND "createdAt" < '2026-05-13T07:00:00Z'
        AND "createdAt" > '2026-05-12T15:00:00Z'
      ORDER BY "createdAt" ASC
    `,
    {},
  )

  return stuckMerges
}


async function triggerMerge(
  temporal: any,
  qx: any,
  primaryId: string,
  secondaryId: string,
): Promise<void> {
  const primary = await findMemberById(qx, primaryId, [MemberField.ID, MemberField.DISPLAY_NAME])
  const secondary = await findMemberById(qx, secondaryId, [
    MemberField.ID,
    MemberField.DISPLAY_NAME,
  ])

  if (!primary) {
    throw new Error(`Primary member not found: ${primaryId}`)
  }
  if (!secondary) {
    throw new Error(`Secondary member not found: ${secondaryId} - may have been deleted during previous merge attempt`)
  }

  // Use unique workflow ID to avoid conflict with terminated workflows
  const workflowId = `finishMemberMerging/${primaryId}/${secondaryId}/retry-${Date.now()}`

  await temporal.workflow.start('finishMemberMerging', {
    taskQueue: 'entity-merging',
    workflowId,
    retry: {
      maximumAttempts: 10,
    },
    args: [
      primary.id,
      secondary.id,
      primary.displayName,
      secondary.displayName,
      '00000000-0000-0000-0000-000000000000',
    ],
    searchAttributes: {
      TenantId: [DEFAULT_TENANT_ID],
    },
  })
}

setImmediate(async () => {
  const dryRun = !parameters.execute

  log.info(
    dryRun
      ? 'DRY RUN MODE - No changes will be made. Use --execute to actually retry.'
      : 'EXECUTE MODE - Will retry stuck merges!',
  )

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  try {
    const stuckMerges = await findStuckMerges(qx)

    log.info(`Found ${stuckMerges.length} stuck merge(s) to retry`)

    if (stuckMerges.length === 0) {
      log.info('No stuck merges found. Nothing to do.')
      process.exit(0)
    }

    let successCount = 0
    let errorCount = 0

    for (const merge of stuckMerges) {
      const { primaryId, secondaryId, createdAt } = merge

      log.info({ primaryId, secondaryId, createdAt }, 'Processing stuck merge...')

      try {
        if (!dryRun) {
          await triggerMerge(temporal, qx, primaryId, secondaryId)
          log.info({ primaryId, secondaryId }, 'Triggered new finishMemberMerging workflow')
        } else {
          log.info({ primaryId, secondaryId }, 'DRY RUN: Would trigger workflow')
        }

        successCount++
      } catch (err) {
        const errorDetails = {
          primaryId,
          secondaryId,
          error: err instanceof Error ? err.message : String(err),
          stack: err instanceof Error ? err.stack : undefined,
        }
        log.error(errorDetails, 'Failed to process stuck merge!')
        errorCount++
      }
    }

    log.info(
      { successCount, errorCount, total: stuckMerges.length },
      `Retry processing complete (${dryRun ? 'DRY RUN' : 'EXECUTED'})`,
    )
  } catch (err) {
    log.error({ err }, 'Unexpected error in retry script!')
    process.exit(1)
  }

  process.exit(0)
})
