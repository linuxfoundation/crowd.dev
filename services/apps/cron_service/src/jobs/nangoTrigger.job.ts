import CronTime from 'cron-time-generator'

import { ConcurrencyLimiter, IS_DEV_ENV } from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  fetchNangoIntegrationDataForCheck,
  fetchNangoLastCheckedAt,
  getNangoMappingsForIntegrations,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  ALL_NANGO_INTEGRATIONS,
  INangoConnectionToCheck,
  NANGO_INTEGRATION_CONFIG,
  NangoIntegration,
  nangoIntegrationToPlatform,
  platformToNangoIntegration,
} from '@crowd/nango'
import {
  TEMPORAL_CONFIG,
  WorkflowIdConflictPolicy,
  WorkflowIdReusePolicy,
  getTemporalClient,
} from '@crowd/temporal'
import { PlatformType } from '@crowd/types'

import { IJobDefinition } from '../types'

// How old an integration must be before we reduce its check frequency
const AGE_THRESHOLD_MS = IS_DEV_ENV
  ? 20 * 60 * 1000 // 20 minutes for local testing
  : 30 * 24 * 60 * 60 * 1000 // 1 month

// Minimum interval between checks for new integrations
const NEW_INTERVAL_MS = IS_DEV_ENV
  ? 5 * 60 * 1000 // 5 minutes
  : 60 * 60 * 1000 // 1 hour

// Minimum interval between checks for old integrations
const OLD_INTERVAL_MS = IS_DEV_ENV
  ? 15 * 60 * 1000 // 15 minutes
  : 6 * 60 * 60 * 1000 // 6 hours

const job: IJobDefinition = {
  name: 'nango-trigger',
  cronTime: IS_DEV_ENV ? CronTime.every(5).minutes() : CronTime.everyHour(),
  timeout: 4 * 60 * 60, // 4 hours
  process: async (ctx) => {
    ctx.log.info('Triggering nango API check as if a webhook was received!')

    const temporal = await getTemporalClient(TEMPORAL_CONFIG())

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)
    const qx = pgpQx(dbConnection)

    const platforms = [...new Set(ALL_NANGO_INTEGRATIONS.map(nangoIntegrationToPlatform))]

    const allIntegrations = await fetchNangoIntegrationDataForCheck(qx, platforms)

    // Batch-fetch lastCheckedAt for all connections
    const lastCheckedAtRows = await fetchNangoLastCheckedAt(qx, platforms)
    const lastCheckedAtMap = new Map<string, string | null>()
    for (const row of lastCheckedAtRows) {
      lastCheckedAtMap.set(`${row.integrationId}/${row.connectionId}`, row.lastCheckedAt)
    }

    const now = new Date()
    const limiter = new ConcurrencyLimiter(5)
    const workflowStarts: Array<() => Promise<void>> = []
    let skippedConnections = 0

    for (let i = 0; i < allIntegrations.length; i++) {
      const int = allIntegrations[i]
      const { id, settings } = int

      const platform = platformToNangoIntegration(int.platform as PlatformType, settings)

      // For GitHub integrations, fetch nango mappings from the dedicated table
      let nangoMapping: Record<
        string,
        { owner: string; repoName: string; repositoryId: string | null }
      > = {}
      if (platform === NangoIntegration.GITHUB) {
        const allNangoMappings = await getNangoMappingsForIntegrations(qx, [id])
        nangoMapping = allNangoMappings[id] || {}
        if (Object.keys(nangoMapping).length === 0) {
          // ignore non-nango github integrations
          continue
        }
      }

      const integrationAgeMs = now.getTime() - new Date(int.createdAt).getTime()
      const isOld = integrationAgeMs >= AGE_THRESHOLD_MS
      const requiredInterval = isOld ? OLD_INTERVAL_MS : NEW_INTERVAL_MS

      // Determine connectionIds for this integration
      const connectionIds: string[] =
        platform === NangoIntegration.GITHUB ? Object.keys(nangoMapping) : [id]

      const models = Object.values(NANGO_INTEGRATION_CONFIG[platform].models) as string[]
      const connections: INangoConnectionToCheck[] = []

      for (const connectionId of connectionIds) {
        const key = `${id}/${connectionId}`
        const lastCheckedAt = lastCheckedAtMap.get(key)

        // Skip if checked recently enough
        if (lastCheckedAt) {
          const elapsed = now.getTime() - new Date(lastCheckedAt).getTime()
          if (elapsed < requiredInterval) {
            skippedConnections++
            continue
          }
        }

        ctx.log.info(
          `${i + 1}/${allIntegrations.length} Triggering nango integration check for ${id} / ${connectionId} (${platform})`,
        )

        let workflowIdPrefix = ''
        if (platform === NangoIntegration.GITHUB) {
          const mapping = nangoMapping[connectionId]
          workflowIdPrefix = `${mapping.owner}/${mapping.repoName}/${connectionId}`
        }

        connections.push({ connectionId, models, workflowIdPrefix })
      }

      if (connections.length > 0) {
        const workflowId = `nango-trigger/${platform}/${id}/cron-triggered`

        workflowStarts.push(async () => {
          await temporal.workflow.start('triggerNangoIntegrationCheck', {
            taskQueue: 'nango',
            workflowId,
            workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
            workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
            retry: {
              maximumAttempts: 10,
            },
            args: [{ integrationId: id, providerConfigKey: platform, connections }],
          })
        })
      }
    }

    ctx.log.info(
      `Triggering ${workflowStarts.length} workflows (skipped ${skippedConnections} connections due to recent checks)`,
    )

    // Track completed workflows
    let completedWorkflows = 0

    // Register callback to track completed workflows
    limiter.setOnJobComplete(() => {
      completedWorkflows++
      if (completedWorkflows % 100 === 0) {
        ctx.log.info(`Triggered ${completedWorkflows} nango integrations checks so far...`)
      }
    })

    // Process all workflow starts with concurrency limit
    for (const workflowStart of workflowStarts) {
      await limiter.schedule(workflowStart)
    }

    // Wait for all remaining jobs to complete
    await limiter.waitForFinish()

    ctx.log.info(`Triggered ${completedWorkflows} nango integrations checks in total`)
  },
}

export default job
