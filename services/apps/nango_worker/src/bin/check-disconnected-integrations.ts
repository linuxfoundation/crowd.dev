import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { fetchNangoDeletedIntegrationData } from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import {
  ALL_NANGO_INTEGRATIONS,
  NangoIntegration,
  deleteNangoConnection,
  getNangoConnections,
  initNangoCloudClient,
  nangoIntegrationToPlatform,
} from '@crowd/nango'

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

let deleteDisconnectedIntegrations = false
if (processArguments.includes('--delete')) {
  deleteDisconnectedIntegrations = true
}

setImmediate(async () => {
  const db = await getDbConnection(READ_DB_CONFIG())

  await initNangoCloudClient()

  const deletedNangoIntegrations = await fetchNangoDeletedIntegrationData(pgpQx(db), [
    ...new Set(
      ALL_NANGO_INTEGRATIONS.map(nangoIntegrationToPlatform).filter(
        (platform) => platform !== null,
      ),
    ),
  ])

  const nangoConnections = await getNangoConnections()

  const deletedIntegrationIds = new Set(deletedNangoIntegrations.map((int) => int.id))

  for (const nangoConnection of nangoConnections.filter(
    (c) => !c.connection_id.startsWith('github-token-'),
  )) {
    if (deletedIntegrationIds.has(nangoConnection.connection_id)) {
      const integrationId = nangoConnection.connection_id
      if (deleteDisconnectedIntegrations) {
        log.warn(
          `Connection ${nangoConnection.connection_id} (${nangoConnection.provider_config_key}) is connected to a deleted integration ${integrationId} - deleting it...`,
        )

        await deleteNangoConnection(
          nangoConnection.provider_config_key as NangoIntegration,
          nangoConnection.connection_id,
        )
      } else {
        log.info(
          `Connection ${nangoConnection.connection_id} (${nangoConnection.provider_config_key}) is connected to a deleted integration ${integrationId} and should be deleted!`,
        )
      }
    }
  }

  process.exit(0)
})
