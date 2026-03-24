import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  fetchNangoDeletedIntegrationData,
  getNangoMappingsForIntegrations,
} from '@crowd/data-access-layer/src/integrations'
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
import { PlatformType } from '@crowd/types'

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

  const qx = pgpQx(db)
  const connectionIds: string[] = []
  // Map connectionId -> integrationId to track which integration each connection belongs to
  const connectionToIntegrationMap = new Map<string, string>()

  for (const int of deletedNangoIntegrations) {
    if (int.platform === PlatformType.GITHUB_NANGO) {
      const allNangoMappings = await getNangoMappingsForIntegrations(qx, [int.id])
      const nangoMapping = allNangoMappings[int.id] || {}
      const connectionIdsForIntegration = Object.keys(nangoMapping)
      if (connectionIdsForIntegration.length > 0) {
        connectionIds.push(...connectionIdsForIntegration)
        // Map each connection ID to this integration ID
        for (const connectionId of connectionIdsForIntegration) {
          connectionToIntegrationMap.set(connectionId, int.id)
        }
      } else {
        log.info(
          { integrationId: int.id, platform: int.platform },
          'Integration has no nango_mapping rows, skipping',
        )
      }
    } else {
      connectionIds.push(int.id)
      // For non-GitHub integrations, the connection_id is the integration ID
      connectionToIntegrationMap.set(int.id, int.id)
    }
  }

  for (const nangoConnection of nangoConnections.filter(
    (c) => !c.connection_id.startsWith('github-token-'),
  )) {
    if (connectionIds.includes(nangoConnection.connection_id)) {
      const integrationId = connectionToIntegrationMap.get(nangoConnection.connection_id)
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
