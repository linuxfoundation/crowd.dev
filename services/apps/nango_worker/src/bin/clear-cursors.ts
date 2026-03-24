import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  clearNangoCursors,
  findIntegrationDataForNangoWebhookProcessing,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

if (processArguments.length !== 1) {
  log.error('Expected 1 argument: integrationIds')
  process.exit(1)
}

const integrationIds = processArguments[0].split(',')

setImmediate(async () => {
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())

  for (const integrationId of integrationIds) {
    const integration = await findIntegrationDataForNangoWebhookProcessing(
      pgpQx(dbConnection),
      integrationId,
    )

    if (integration) {
      log.info(`Clearing cursors for integration '${integrationId} (${integration.platform})'!`)
      await clearNangoCursors(pgpQx(dbConnection), integrationId)
    }
  }

  process.exit(0)
})
