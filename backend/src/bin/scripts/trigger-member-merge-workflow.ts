import commandLineArgs from 'command-line-args'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { MemberField, findMemberById, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { queryMergeActions } from '@crowd/data-access-layer/src/mergeActions/repo'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'
import { MergeActionState } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'primaryId',
    alias: 'p',
    typeLabel: '{underline primaryId}',
    type: String,
    description: 'The unique ID of the primary member.',
  },
  {
    name: 'secondaryId',
    alias: 's',
    typeLabel: '{underline secondaryId}',
    type: String,
    description: 'The unique ID of the secondary member.',
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
  const primaryId = parameters.primaryId
  const secondaryId = parameters.secondaryId

  if (!primaryId || !secondaryId) {
    log.error('Primary and secondary IDs are required!')
    process.exit(1)
  }

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  log.info({ primaryId, secondaryId }, 'Triggering member merge workflow!')

  try {
    const primary = await findMemberById(qx, primaryId, [MemberField.ID, MemberField.DISPLAY_NAME])
    const secondary = await findMemberById(qx, secondaryId, [
      MemberField.ID,
      MemberField.DISPLAY_NAME,
    ])

    if (!primary || !secondary) {
      log.error('Primary or secondary member not found!')
      process.exit(1)
    }

    // check if any ongoing merge action exists
    const mergeActions = await queryMergeActions(qx, {
      fields: ['id'],
      filter: {
        and: [
          {
            state: {
              eq: MergeActionState.IN_PROGRESS,
            },
          },
          {
            and: [{ primaryId: { eq: primaryId } }, { secondaryId: { eq: secondaryId } }],
          },
        ],
      },
      limit: 1,
      orderBy: '"updatedAt" DESC',
    })

    if (mergeActions.length > 0) {
      log.error(
        { primaryId, secondaryId },
        'Member merge already in progress. Resolve the existing merge before retrying!',
      )

      process.exit(1)
    }

    await temporal.workflow.start('finishMemberMerging', {
      taskQueue: 'entity-merging',
      workflowId: `finishMemberMerging/${primaryId}/${secondaryId}`,
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
  } catch (err) {
    log.error({ primaryId, secondaryId, err }, 'Failed to trigger member merge workflow!')
    process.exit(1)
  }

  process.exit(0)
})
