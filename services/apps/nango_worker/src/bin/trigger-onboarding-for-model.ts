import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  clearNangoCursorForModel,
  getNangoMappingByConnectionId,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { INangoWebhookPayload, platformToNangoIntegration } from '@crowd/nango'
import { TEMPORAL_CONFIG, WorkflowIdReusePolicy, getTemporalClient } from '@crowd/temporal'
import { PlatformType } from '@crowd/types'

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

if (processArguments.length !== 3) {
  log.error('Expected 3 arguments: integrationId, connectionId and model')
  process.exit(1)
}

const [integrationId, connectionId, model] = processArguments

setImmediate(async () => {
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const temporal = await getTemporalClient(TEMPORAL_CONFIG())

  const integration = await dbConnection.oneOrNone(
    `select * from integrations where id = $(integrationId) and "deletedAt" is null`,
    {
      integrationId,
    },
  )

  if (integration) {
    const qx = pgpQx(dbConnection)
    const nangoMappingRow = await getNangoMappingByConnectionId(qx, connectionId)
    if (
      integration.id === connectionId ||
      (nangoMappingRow && nangoMappingRow.integrationId === integration.id)
    ) {
      log.info(
        `Triggering nango integration check for integrationId '${integrationId}' and connectionId '${connectionId}'!`,
      )
      const providerConfigKey = platformToNangoIntegration(
        integration.platform as PlatformType,
        integration.settings,
      )

      await clearNangoCursorForModel(pgpQx(dbConnection), integrationId, connectionId, model)

      const payload: INangoWebhookPayload = {
        connectionId: connectionId,
        providerConfigKey,
        syncName: 'not important',
        model,
        responseResults: { added: 1, updated: 1, deleted: 1 },
        syncType: 'INCREMENTAL',
        modifiedAfter: new Date().toISOString(),
      }

      await temporal.workflow.start('processNangoWebhook', {
        taskQueue: 'nango',
        workflowId: `nango-webhook/${providerConfigKey}/${connectionId}/${model}/script-triggered`,
        workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
        retry: {
          maximumAttempts: 10,
        },
        args: [payload],
      })
    } else {
      log.error(`Integration ${integrationId} does not have connectionId ${connectionId}!`)
    }
  } else {
    log.error(`Integration not found for id: ${integrationId}!`)
  }

  process.exit(0)
})
