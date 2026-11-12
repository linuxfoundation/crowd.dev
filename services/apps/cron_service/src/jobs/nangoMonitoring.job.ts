import CronTime from 'cron-time-generator'

import {
  ConcurrencyLimiter,
  IS_DEV_ENV,
  IS_PROD_ENV,
  distinctBy,
  singleOrDefault,
} from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  INangoIntegrationData,
  fetchNangoIntegrationData,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  ALL_NANGO_INTEGRATIONS,
  NangoIntegration,
  SyncStatus,
  getNangoConnectionStatus,
  getNangoConnections,
  initNangoCloudClient,
  nangoIntegrationToPlatform,
  platformToNangoIntegration,
} from '@crowd/nango'
import {
  SlackChannel,
  SlackMessageSection,
  SlackPersona,
  sendSlackNotificationAsync,
} from '@crowd/slack'
import { PlatformType } from '@crowd/types'

import { IJobDefinition } from '../types'

/* eslint-disable @typescript-eslint/no-explicit-any */

const nangoEnv = IS_PROD_ENV ? 'prod' : IS_DEV_ENV ? 'local' : 'dev'

const STATUS_CHECK_CONCURRENCY = 10

type NangoIntegrationDataExtended = INangoIntegrationData & { connectionId: string }

const job: IJobDefinition = {
  name: 'nango-monitor',
  cronTime: IS_DEV_ENV ? CronTime.everyMinute() : CronTime.everyDayAt(8, 0),
  timeout: 60 * 60,
  process: async (ctx) => {
    ctx.log.info('Running nango-monitor job...')

    await initNangoCloudClient()
    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)

    const nonGithubIntegrations = ALL_NANGO_INTEGRATIONS.filter(
      (integration) => integration !== NangoIntegration.GITHUB,
    )

    const allIntegrations = await fetchNangoIntegrationData(pgpQx(dbConnection), [
      ...new Set(nonGithubIntegrations.map(nangoIntegrationToPlatform)),
    ])

    const nangoConnections = await getNangoConnections()

    const statusMap = new Map<NangoIntegrationDataExtended, SyncStatus[]>()

    let failedStatusChecks = 0

    // Collect all status check operations
    const statusCheckOperations: Array<() => Promise<void>> = []

    for (const int of allIntegrations) {
      const nangoConnection = singleOrDefault(nangoConnections, (c) => c.connection_id === int.id)
      if (!nangoConnection) {
        ctx.log.warn(`${int.platform} integration with id "${int.id}" is not connected to Nango!`)
      } else {
        statusCheckOperations.push(async () => {
          try {
            const results = await getNangoConnectionStatus(
              platformToNangoIntegration(int.platform as PlatformType, int.settings),
              nangoConnection.connection_id,
            )
            if (results) {
              statusMap.set(
                {
                  ...int,
                  connectionId: nangoConnection.connection_id,
                },
                results,
              )
            }
          } catch (error) {
            failedStatusChecks++
            ctx.log.error(
              { error, connectionId: nangoConnection.connection_id, integrationId: int.id },
              `Failed to get Nango connection status for ${int.platform} integration ${int.id}`,
            )
          }
        })
      }
    }

    ctx.log.info(
      `Fetching status for ${statusCheckOperations.length} Nango connections (${STATUS_CHECK_CONCURRENCY} at a time)...`,
    )

    const limiter = new ConcurrencyLimiter(STATUS_CHECK_CONCURRENCY)
    let completedChecks = 0

    limiter.setOnJobComplete(() => {
      completedChecks++
      if (completedChecks % 20 === 0) {
        ctx.log.info(
          `Completed ${completedChecks}/${statusCheckOperations.length} status checks...`,
        )
      }
    })

    for (const operation of statusCheckOperations) {
      await limiter.schedule(operation)
    }

    await limiter.waitForFinish()

    ctx.log.info(`Completed all ${completedChecks} status checks`)

    ctx.log.info(
      `Found ${distinctBy(Array.from(statusMap.keys()), (k) => k.id).length} integrations that are mapped in nango cloud with ${statusMap.size} connections.`,
    )

    // Send one Slack notification per platform
    const notificationPromises: Promise<void | boolean>[] = []

    for (const nangoIntegration of nonGithubIntegrations) {
      const integrations = Array.from(statusMap.entries()).filter(
        (i) => i[0].platform === nangoIntegrationToPlatform(nangoIntegration),
      )

      if (integrations.length > 0) {
        let successfulSyncs = 0
        let failedSyncs = 0
        let runningSyncs = 0

        const failedConnections: NangoIntegrationDataExtended[] = []
        for (const [data, statuses] of integrations) {
          const failed = statuses.filter((s) => s.status === 'ERROR')
          successfulSyncs += statuses.filter((s) => s.status === 'SUCCESS').length
          failedSyncs += failed.length
          runningSyncs += statuses.filter((s) => s.status === 'RUNNING').length

          if (failed.length > 0) {
            failedConnections.push(data)
          }
        }

        const sections: SlackMessageSection[] = []

        // Summary section
        let summaryText = `${distinctBy(integrations, (i) => i[0].id).length} integrations • ${integrations.length} connections • ${successfulSyncs + failedSyncs + runningSyncs} syncs\n\n`

        const syncMessages: string[] = []
        if (successfulSyncs > 0) {
          syncMessages.push(`✅ ${successfulSyncs} successful`)
        }
        if (failedSyncs > 0) {
          syncMessages.push(`❌ ${failedSyncs} failed`)
        }
        if (runningSyncs > 0) {
          syncMessages.push(`⏳ ${runningSyncs} running`)
        }

        if (syncMessages.length > 0) {
          summaryText += syncMessages.join(' • ')
        }

        sections.push({
          title: 'Summary',
          text: summaryText,
        })

        // Failed connections with links (limit to first 10)
        if (failedConnections.length > 0) {
          const maxToShow = 10
          const connectionsToShow = failedConnections.slice(0, maxToShow)
          const remainingCount = failedConnections.length - maxToShow

          let failedText = `*Total:* ${failedConnections.length} failed connection${failedConnections.length > 1 ? 's' : ''}\n\n${connectionsToShow.map((failed) => `• <https://app.nango.dev/${nangoEnv}/connections/${platformToNangoIntegration(failed.platform as PlatformType, failed.settings)}/${failed.connectionId}|${failed.connectionId}>`).join('\n')}`

          if (remainingCount > 0) {
            failedText += `\n\n_... and ${remainingCount} more_`
          }

          sections.push({
            title: 'Failed Connections',
            text: failedText,
          })
        }

        // Determine persona for this platform
        const persona =
          failedSyncs > 0 ? SlackPersona.WARNING_PROPAGATOR : SlackPersona.INFO_NOTIFIER

        // Queue notification for this platform
        notificationPromises.push(
          sendSlackNotificationAsync(
            SlackChannel.CDP_INTEGRATIONS_ALERTS,
            persona,
            `Nango Monitor: ${nangoIntegration}`,
            sections,
          ).catch((error) => {
            ctx.log.error(
              { error, platform: nangoIntegration },
              `Failed to send Slack notification for ${nangoIntegration}`,
            )
          }),
        )
      }
    }

    // Send API errors summary if needed
    if (failedStatusChecks > 0) {
      notificationPromises.push(
        sendSlackNotificationAsync(
          SlackChannel.CDP_INTEGRATIONS_ALERTS,
          SlackPersona.ERROR_REPORTER,
          'Nango Monitor: API Errors',
          `Failed to retrieve status for ${failedStatusChecks} connection${failedStatusChecks > 1 ? 's' : ''} due to Nango API errors.\n\nCheck logs for details.`,
        ).catch((error) => {
          ctx.log.error({ error }, 'Failed to send API errors Slack notification')
        }),
      )
    }

    // Wait for all notifications to complete
    await Promise.all(notificationPromises)
    ctx.log.info(
      `Sent ${notificationPromises.length} Slack notification${notificationPromises.length > 1 ? 's' : ''} to NANGO_ALERTS channel`,
    )
  },
}

export default job
