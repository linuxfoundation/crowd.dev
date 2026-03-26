import { executeChild, proxyActivities } from '@temporalio/workflow'

import { PlatformType } from '@crowd/types'

import type * as activities from '../activities/exportActivity'
import type { DataSourceName } from '../integrations/types'

const { getEnabledPlatforms, getDataSourceNamesForPlatform } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
})

const { executeExport } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 hour',
  retry: {
    initialInterval: '2s',
    backoffCoefficient: 2,
    maximumInterval: '60s',
    maximumAttempts: 3,
  },
})

export async function snowflakeS3ExportScheduler(): Promise<void> {
  const platforms = await getEnabledPlatforms()

  const childWorkflows: Promise<void>[] = []

  for (const platform of platforms) {
    const sourceNames = await getDataSourceNamesForPlatform(platform)

    for (const sourceName of sourceNames) {
      const now = new Date().toISOString().slice(0, 19)
      childWorkflows.push(
        executeChild('exportSnowflakeData', {
          workflowId: `snowflake-export/${platform}/${sourceName}/${now}`,
          args: [platform, sourceName],
          retry: {
            initialInterval: '2s',
            backoffCoefficient: 2,
            maximumAttempts: 3,
          },
        }),
      )
    }
  }

  await Promise.allSettled(childWorkflows)
}

export async function exportSnowflakeData(
  platform: PlatformType,
  sourceName: DataSourceName,
): Promise<void> {
  await executeExport(platform, sourceName)
}
