import commandLineArgs from 'command-line-args'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { OrganizationField, findOrgById, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { queryMergeActions } from '@crowd/data-access-layer/src/mergeActions/repo'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'
import { MergeActionState } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'originalId',
    alias: 'o',
    typeLabel: '{underline originalId}',
    type: String,
    description: 'The unique ID of the primary organization (will be kept).',
  },
  {
    name: 'toMergeId',
    alias: 'm',
    typeLabel: '{underline toMergeId}',
    type: String,
    description: 'The unique ID of the organization to merge into the primary (will be destroyed).',
  },
  {
    name: 'blockAffiliations',
    alias: 'b',
    type: Boolean,
    defaultValue: false,
    description: 'Whether to block affiliations during merge. Defaults to false.',
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
  const originalId = parameters.originalId
  const toMergeId = parameters.toMergeId
  const blockAffiliations = parameters.blockAffiliations ?? false

  if (!originalId || !toMergeId) {
    log.error('Original and toMerge IDs are required!')
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

  log.info({ originalId, toMergeId }, 'Triggering organization merge workflow!')

  try {
    const original = await findOrgById(qx, originalId, [
      OrganizationField.ID,
      OrganizationField.DISPLAY_NAME,
      OrganizationField.IS_AFFILIATION_BLOCKED
    ])
    const toMerge = await findOrgById(qx, toMergeId, [
      OrganizationField.ID,
      OrganizationField.DISPLAY_NAME,
    ])

    if (!original || !toMerge) {
      log.error('Original or toMerge organization not found!')
      process.exit(1)
    }

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
            and: [{ primaryId: { eq: originalId } }, { secondaryId: { eq: toMergeId } }],
          },
        ],
      },
      limit: 1,
      orderBy: '"updatedAt" DESC',
    })

    if (mergeActions.length > 0) {
      log.error(
        { originalId, toMergeId },
        'Organization merge already in progress. Resolve the existing merge before retrying!',
      )

      process.exit(1)
    }

    await temporal.workflow.start('finishOrganizationMerging', {
      taskQueue: 'entity-merging',
      workflowId: `finishOrganizationMerging/${originalId}/${toMergeId}`,
      retry: {
        maximumAttempts: 10,
      },
      args: [
        originalId,
        toMergeId,
        original.displayName,
        toMerge.displayName,
        blockAffiliations,
        '00000000-0000-0000-0000-000000000000',
      ],
      searchAttributes: {
        TenantId: [DEFAULT_TENANT_ID],
      },
    })

    log.info({ originalId, toMergeId }, 'Organization merge workflow triggered successfully!')
  } catch (err) {
    log.error({ originalId, toMergeId, err }, 'Failed to trigger organization merge workflow!')
    process.exit(1)
  }

  process.exit(0)
})
