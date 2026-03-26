import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  clearNangoCursors,
  findIntegrationDataForNangoWebhookProcessing,
  getNangoMappingsForIntegrations,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { initNangoCloudClient, platformToNangoIntegration, startNangoSync } from '@crowd/nango'
import { PlatformType } from '@crowd/types'

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

if (processArguments.length !== 1) {
  log.error('Expected 1 argument: integrationIds')
  process.exit(1)
}

const integrationIds = processArguments[0].split(',')

setImmediate(async () => {
  await initNangoCloudClient()
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())

  for (const integrationId of integrationIds) {
    const integration = await findIntegrationDataForNangoWebhookProcessing(
      pgpQx(dbConnection),
      integrationId,
    )

    if (integration) {
      const nangoIntegration = platformToNangoIntegration(
        integration.platform as PlatformType,
        integration.settings,
      )

      try {
        const toTrigger: string[] = []
        if (integration.platform === PlatformType.GITHUB_NANGO) {
          const allNangoMappings = await getNangoMappingsForIntegrations(pgpQx(dbConnection), [
            integration.id,
          ])
          toTrigger.push(...Object.keys(allNangoMappings[integration.id] || {}))
        } else if (integration.platform === PlatformType.GERRIT) {
          toTrigger.push(integration.id)
        } else {
          throw new Error(`Unsupported platform: ${integration.platform}!`)
        }

        for (const nangoConnectionId of toTrigger) {
          log.info(
            `Restarting syncs for connectionId '${nangoConnectionId}' from integration '${integrationId} (${integration.platform})'!`,
          )

          await startNangoSync(nangoIntegration, nangoConnectionId, undefined, true)
        }

        // clear cursors
        log.info(`Clearing cursors for integration '${integrationId} (${integration.platform})'!`)
        await clearNangoCursors(pgpQx(dbConnection), integrationId)
      } catch (error) {
        log.error(`Failed to restart syncs for integration: ${integrationId}`, error)
      }
    }
  }

  process.exit(0)
})
