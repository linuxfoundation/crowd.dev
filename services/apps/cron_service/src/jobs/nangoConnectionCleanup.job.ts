import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_STAGING_ENV, singleOrDefault } from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  INangoIntegrationData,
  fetchNangoDeletedIntegrationData,
  fetchNangoIntegrationData,
  getNangoMappingByConnectionId,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  ALL_NANGO_INTEGRATIONS,
  NangoIntegration,
  deleteNangoConnection,
  getNangoConnections,
  initNangoCloudClient,
  nangoIntegrationToPlatform,
} from '@crowd/nango'
import { PlatformType } from '@crowd/types'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'nango-connection-cleanup',
  cronTime: IS_DEV_ENV ? CronTime.every(10).minutes() : CronTime.everyWeek(),
  timeout: 15 * 60,
  enabled: async () => IS_DEV_ENV || IS_STAGING_ENV,
  process: async (ctx) => {
    ctx.log.info('Cleaning up stale/old/unconnected nango integration connections!')

    await initNangoCloudClient()
    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)
    const qx = pgpQx(dbConnection)

    // Fetch all active integrations
    const activeIntegrations = await fetchNangoIntegrationData(qx, [
      ...new Set(ALL_NANGO_INTEGRATIONS.map(nangoIntegrationToPlatform)),
    ])

    // Fetch deleted integrations to clean up their connections
    const deletedIntegrations = await fetchNangoDeletedIntegrationData(qx, [
      ...new Set(ALL_NANGO_INTEGRATIONS.map(nangoIntegrationToPlatform)),
    ])

    // Build a set of deleted integration IDs for quick lookup
    const deletedIntegrationIds = new Set(deletedIntegrations.map((i) => i.id))

    // Combine for lookup purposes
    const allIntegrations = [...activeIntegrations, ...deletedIntegrations]

    const nangoConnections = await getNangoConnections()

    for (const connection of nangoConnections.filter((c) =>
      ALL_NANGO_INTEGRATIONS.includes(c.provider_config_key as NangoIntegration),
    )) {
      // ignore github token connections
      if (connection.connection_id.toLowerCase().startsWith('github-token-')) {
        continue
      }

      let integration: INangoIntegrationData | undefined

      // github integrations have connection ids per repo in the integration.nango_mapping table
      if (connection.provider_config_key === NangoIntegration.GITHUB) {
        const mapping = await getNangoMappingByConnectionId(qx, connection.connection_id)
        if (mapping) {
          integration = singleOrDefault(
            allIntegrations,
            (i) => i.id === mapping.integrationId && i.platform === PlatformType.GITHUB_NANGO,
          )
        }
        // for other integrations, check if the connection belongs to an integration
      } else {
        integration = singleOrDefault(allIntegrations, (i) => i.id === connection.connection_id)
      }

      const integrationCreatedAt = new Date(connection.created)
      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)

      const shouldDelete =
        integrationCreatedAt < thirtyDaysAgo &&
        // If connection doesn't belong to any integration, delete after 30 days
        (!integration ||
          // If connection belongs to a deleted integration, delete after 30 days
          (integration && deletedIntegrationIds.has(integration?.id)))

      if (shouldDelete) {
        ctx.log.info(`Deleting stale connection ${connection.connection_id}`)
        await deleteNangoConnection(
          connection.provider_config_key as NangoIntegration,
          connection.connection_id,
        )
      }
    }
  },
}

export default job
