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
  fetchNangoCursorRowsForIntegration,
  fetchNangoIntegrationData,
  getNangoMappingsForIntegrations,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getReposForGithubIntegration } from '@crowd/data-access-layer/src/repositories'
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

type NangoIntegrationDataExtended = INangoIntegrationData & { connectionId: string }

const job: IJobDefinition = {
  name: 'nango-monitor',
  cronTime: IS_DEV_ENV ? CronTime.everyMinute() : CronTime.everyDayAt(8, 0),
  timeout: 60 * 60,
  process: async (ctx) => {
    ctx.log.info('Running nango-monitor job...')

    await initNangoCloudClient()
    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)

    const allIntegrations = await fetchNangoIntegrationData(pgpQx(dbConnection), [
      ...new Set(ALL_NANGO_INTEGRATIONS.map(nangoIntegrationToPlatform)),
    ])

    const nangoConnections = await getNangoConnections()

    const statusMap = new Map<NangoIntegrationDataExtended, SyncStatus[]>()

    const ghMissingNangoConnections: Map<string, string[]> = new Map()
    const ghNotConnectedToNangoYet: Map<string, number> = new Map()
    const ghNoCursorsYet: Map<string, number> = new Map()

    let totalRepos = 0
    let failedStatusChecks = 0

    // Collect all status check operations
    const statusCheckOperations: Array<() => Promise<void>> = []

    for (const int of allIntegrations) {
      if (int.platform === PlatformType.GITHUB_NANGO) {
        // Fetch nango mappings from the dedicated table
        const allNangoMappings = await getNangoMappingsForIntegrations(pgpQx(dbConnection), [
          int.id,
        ])
        const nangoMapping = allNangoMappings[int.id] || {}

        // Check which repos are connected to nango by comparing repositories table vs nango_mapping
        const repoRows = await getReposForGithubIntegration(pgpQx(dbConnection), int.id)
        for (const repo of repoRows) {
          totalRepos++

          let found = false
          for (const mapping of Object.values(nangoMapping)) {
            if (mapping.owner === repo.owner && mapping.repoName === repo.name) {
              found = true
            }
          }

          if (!found) {
            if (ghNotConnectedToNangoYet.has(int.id)) {
              ghNotConnectedToNangoYet.set(int.id, ghNotConnectedToNangoYet.get(int.id) + 1)
            } else {
              ghNotConnectedToNangoYet.set(int.id, 1)
            }
          }
        }

        // then collect nango connection status checks for each connection
        const connectionIds = Object.keys(nangoMapping)
        if (connectionIds.length > 0) {
          const cursorRows = await fetchNangoCursorRowsForIntegration(pgpQx(dbConnection), int.id)
          const connectionIdsWithCursors = new Set(cursorRows.map((r) => r.connectionId))

          for (const connectionId of connectionIds) {
            // check if we have cursors already for this connection
            if (!connectionIdsWithCursors.has(connectionId)) {
              if (ghNoCursorsYet.has(int.id)) {
                ghNoCursorsYet.set(int.id, ghNoCursorsYet.get(int.id) + 1)
              } else {
                ghNoCursorsYet.set(int.id, 1)
              }
            }

            const nangoConnection = singleOrDefault(
              nangoConnections,
              (c) => c.connection_id === connectionId,
            )
            if (nangoConnection) {
              statusCheckOperations.push(async () => {
                try {
                  const results = await getNangoConnectionStatus(
                    NangoIntegration.GITHUB,
                    nangoConnection.connection_id,
                  )

                  statusMap.set(
                    {
                      ...int,
                      connectionId,
                    },
                    results,
                  )
                } catch (error) {
                  failedStatusChecks++
                  ctx.log.error(
                    { error, connectionId, integrationId: int.id },
                    `Failed to get Nango connection status for ${connectionId}`,
                  )
                }
              })
            } else {
              // repo not connected to nango anymore!
              if (ghMissingNangoConnections.has(int.id)) {
                ghMissingNangoConnections.get(int.id).push(connectionId)
              } else {
                ghMissingNangoConnections.set(int.id, [connectionId])
              }
            }
          }
        }
      } else {
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
    }

    ctx.log.info(
      `Fetching status for ${statusCheckOperations.length} Nango connections (5 at a time)...`,
    )

    // Execute status checks with concurrency limit of 10
    const limiter = new ConcurrencyLimiter(10)
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
    const notificationPromises: Promise<void>[] = []

    for (const nangoIntegration of ALL_NANGO_INTEGRATIONS) {
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

        // GitHub-specific stats
        if (nangoIntegration === NangoIntegration.GITHUB) {
          const githubIssues: string[] = []

          if (ghMissingNangoConnections.size > 0) {
            for (const [integrationId, connectionIds] of ghMissingNangoConnections.entries()) {
              githubIssues.push(
                `• Integration \`${integrationId}\` has ${connectionIds.length} missing nango connections (${connectionIds.join(',')})`,
              )
            }
          }

          if (ghNotConnectedToNangoYet.size > 0) {
            let totalNotConnected = 0
            for (const count of Array.from(ghNotConnectedToNangoYet.values())) {
              totalNotConnected += count
            }
            githubIssues.push(
              `• ${totalNotConnected} out of ${totalRepos} repos not connected to Nango yet`,
            )
          }

          if (ghNoCursorsYet.size > 0) {
            let totalNoCursors = 0
            for (const count of Array.from(ghNoCursorsYet.values())) {
              totalNoCursors += count
            }
            githubIssues.push(`• ${totalNoCursors} repos have no cursors yet`)
          }

          if (githubIssues.length > 0) {
            sections.push({
              title: 'GitHub Issues',
              text: githubIssues.join('\n'),
            })
          }
        }

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
