import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_PROD_ENV } from '@crowd/common'
import {
  listOctolensKeywordMappingsDeactivatedBetween,
  listOctolensMappingDiscoveryStates,
} from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { REDIS_CONFIG, getRedisClient } from '@crowd/redis'
import { SlackChannel, sendSlackNotificationAsync } from '@crowd/slack'

import { IJobDefinition } from '../types'
import { runOctolensMappingHealthCheck } from './octolensMappingHealth'

const job: IJobDefinition = {
  name: 'octolens-mapping-health-check',
  cronTime: IS_DEV_ENV ? CronTime.every(15).minutes() : CronTime.everyHourAt(30),
  timeout: 5 * 60,
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    ctx.log.info('Running octolens-mapping-health-check job...')

    // Primary, not replica: replica lag beyond the cursor margin could skip deactivations.
    const dbConnection = await getDbConnection(WRITE_DB_CONFIG(), 3, 0)
    const qx = pgpQx(dbConnection)
    const redis = await getRedisClient(REDIS_CONFIG())

    const result = await runOctolensMappingHealthCheck({
      listStates: () => listOctolensMappingDiscoveryStates(qx),
      listDeactivatedBetween: (since, until) =>
        listOctolensKeywordMappingsDeactivatedBetween(qx, since, until),
      getKey: (key) => redis.get(key),
      setKey: async (key, value, expireAfterSeconds) => {
        await redis.set(key, value, expireAfterSeconds ? { EX: expireAfterSeconds } : undefined)
      },
      sendAlert: (alert) =>
        sendSlackNotificationAsync(
          SlackChannel.CDP_INTEGRATIONS_ALERTS,
          alert.persona,
          alert.title,
          alert.sections,
        ),
    })

    if (result.sendFailures > 0) {
      ctx.log.warn('octolens mapping health alert failed to send, will retry next run')
    }

    ctx.log.info(
      `Octolens mapping health check done: integrations=${result.integrations}, staleAlerted=${result.staleAlerted}, deactivationsReported=${result.deactivationsReported}, sendFailures=${result.sendFailures}`,
    )
  },
}

export default job
