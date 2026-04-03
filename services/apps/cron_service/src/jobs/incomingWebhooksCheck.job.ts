import CronTime from 'cron-time-generator'

import { IS_PROD_ENV } from '@crowd/common'
import { IntegrationStreamWorkerEmitter } from '@crowd/common_services'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { QUEUE_CONFIG, getKafkaClient, getKafkaMessageCounts } from '@crowd/queue'
import { KafkaQueueService } from '@crowd/queue/src/vendors/kafka/client'
import { WebhookState } from '@crowd/types'

import { IJobDefinition } from '../types'

const TOPIC = 'integration-stream-worker-high-production'
const GROUP_ID = 'integration-stream-worker-high-production'
const MAX_UNCONSUMED = 50000

const job: IJobDefinition = {
  name: 'incoming-webhooks-check',
  cronTime: CronTime.everyDay(),
  timeout: 30 * 60, // 30 minutes
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    const kafkaClient = getKafkaClient(QUEUE_CONFIG())
    const admin = kafkaClient.admin()
    await admin.connect()

    const counts = await getKafkaMessageCounts(ctx.log, admin, TOPIC, GROUP_ID)

    if (counts.unconsumed >= MAX_UNCONSUMED) {
      ctx.log.info(
        `Integration stream worker queue has ${counts.unconsumed} unconsumed messages, skipping!`,
      )
      return
    }

    const dbConnection = await getDbConnection(WRITE_DB_CONFIG())

    const count = (
      await dbConnection.one(
        `select count(*)::int as count from "incomingWebhooks" where state = $(state) and "createdAt" < now() - interval '1 day'`,
        { state: WebhookState.PENDING },
      )
    ).count

    if (count <= counts.unconsumed) {
      ctx.log.info(`All ${count} stuck pending webhooks are already in the queue, skipping!`)
      return
    }

    const webhooks = await dbConnection.any<{ id: string; platform: string }>(
      `
      select iw.id, i.platform
      from "incomingWebhooks" iw
      join integrations i on iw."integrationId" = i.id
      where iw.state = $(state)
        and iw."createdAt" < now() - interval '1 day'
      order by iw."createdAt" asc
      limit 10000
      `,
      { state: WebhookState.PENDING },
    )

    if (webhooks.length === 0) {
      ctx.log.info('No stuck pending webhooks found!')
      return
    }

    ctx.log.info(`Found ${webhooks.length} stuck pending webhooks, re-triggering!`)

    const queueService = new KafkaQueueService(kafkaClient, ctx.log)
    const emitter = new IntegrationStreamWorkerEmitter(queueService, ctx.log)
    await emitter.init()

    await emitter.triggerWebhookProcessingBatch(webhooks.map((w) => w.id))

    ctx.log.info(`Re-triggered ${webhooks.length} stuck pending webhooks in total!`)
  },
}

export default job
