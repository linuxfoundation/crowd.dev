import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  fetchNangoIntegrationData,
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

let deleteDisconnectedConnections = false
if (processArguments.includes('--delete')) {
  deleteDisconnectedConnections = true
}

setImmediate(async () => {
  const db = await getDbConnection(READ_DB_CONFIG())

  await initNangoCloudClient()

  const nangoIntegrations = await fetchNangoIntegrationData(
    pgpQx(db),
    ALL_NANGO_INTEGRATIONS.map(nangoIntegrationToPlatform),
  )

  const nangoConnections = await getNangoConnections()

  const qx = pgpQx(db)
  const connectionIds: string[] = []
  for (const int of nangoIntegrations) {
    if (int.platform === PlatformType.GITHUB_NANGO) {
      const allNangoMappings = await getNangoMappingsForIntegrations(qx, [int.id])
      connectionIds.push(...Object.keys(allNangoMappings[int.id] || {}))
    } else {
      connectionIds.push(int.id)
    }
  }

  for (const nangoConnection of nangoConnections.filter(
    (c) => !c.connection_id.startsWith('github-token-'),
  )) {
    if (!connectionIds.includes(nangoConnection.connection_id)) {
      if (deleteDisconnectedConnections) {
        log.warn(
          `Connection ${nangoConnection.connection_id} (${nangoConnection.provider_config_key}) is not connected to integration - deleting it...`,
        )

        await deleteNangoConnection(
          nangoConnection.provider_config_key as NangoIntegration,
          nangoConnection.connection_id,
        )
      } else {
        log.warn(
          `Connection ${nangoConnection.connection_id} (${nangoConnection.provider_config_key}) is not connected to integration and should be deleted!`,
        )
      }
    }
  }

  process.exit(0)
})
