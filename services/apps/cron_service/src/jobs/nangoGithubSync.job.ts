import CronTime from 'cron-time-generator'

import { ConcurrencyLimiter, IS_DEV_ENV } from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { fetchNangoIntegrationData } from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { NangoIntegration, nangoIntegrationToPlatform } from '@crowd/nango'
import {
  TEMPORAL_CONFIG,
  WorkflowIdConflictPolicy,
  WorkflowIdReusePolicy,
  getTemporalClient,
} from '@crowd/temporal'
import { PlatformType } from '@crowd/types'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'nango-github-sync',
  cronTime: CronTime.every(
    Number(process.env.CROWD_GH_NANGO_SYNC_INTERVAL_MINUTES || (IS_DEV_ENV ? 5 : 60)),
  ).minutes(),
  timeout: 4 * 60 * 60, // 4 hours
  process: async (ctx) => {
    ctx.log.info('Triggering nango API check as if a webhook was received!')

    const temporal = await getTemporalClient(TEMPORAL_CONFIG())

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)

    const integrations = await fetchNangoIntegrationData(pgpQx(dbConnection), [
      nangoIntegrationToPlatform(NangoIntegration.GITHUB),
    ])

    const limiter = new ConcurrencyLimiter(5)

    // Collect all workflow start operations
    const workflowStarts: Array<() => Promise<void>> = []

    for (let i = 0; i < integrations.length; i++) {
      const int = integrations[i]
      const { id, platform } = int

      if (platform !== PlatformType.GITHUB_NANGO) {
        continue
      }

      ctx.log.info(
        {
          integrationId: id,
        },
        `${i + 1}/${integrations.length} Triggering nango github integration sync!`,
      )

      workflowStarts.push(async () => {
        await temporal.workflow
          .start('syncGithubIntegration', {
            taskQueue: 'nango',
            workflowId: `github-nango-sync/cron/${id}`,
            workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
            workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
            retry: {
              maximumAttempts: 10,
            },
            args: [{ integrationId: id }],
          })
          .catch((err) =>
            ctx.log.error(err, 'Error while triggering nango github integration sync!'),
          )
      })
    }

    ctx.log.info(
      `Triggering nango github integration syncs with ${workflowStarts.length} workflows!`,
    )

    // Track completed workflows
    let completedWorkflows = 0

    // Register callback to track completed workflows
    limiter.setOnJobComplete(() => {
      completedWorkflows++
      if (completedWorkflows % 100 === 0) {
        ctx.log.info(`Triggered ${completedWorkflows} nango github integration syncs so far...`)
      }
    })

    // Process all workflow starts with concurrency limit
    for (const workflowStart of workflowStarts) {
      await limiter.schedule(workflowStart)
    }

    // Wait for all remaining jobs to complete
    await limiter.waitForFinish()

    ctx.log.info(`Triggered ${completedWorkflows} nango github integration syncs in total`)
  },
}

export default job
