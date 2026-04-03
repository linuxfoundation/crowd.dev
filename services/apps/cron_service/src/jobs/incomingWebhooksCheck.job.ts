import CronTime from 'cron-time-generator'

import { IS_PROD_ENV } from '@crowd/common'
import { IntegrationStreamWorkerEmitter } from '@crowd/common_services'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { Logger } from '@crowd/logging'
import { KafkaAdmin, QUEUE_CONFIG, getKafkaClient } from '@crowd/queue'
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

    const counts = await getMessageCounts(ctx.log, admin, TOPIC, GROUP_ID)

    if (counts.unconsumed >= MAX_UNCONSUMED) {
      ctx.log.info(
        `Integration stream worker queue has ${counts.unconsumed} unconsumed messages, skipping!`,
      )
      return
    }

    const dbConnection = await getDbConnection(WRITE_DB_CONFIG())

    const count = (
      await dbConnection.one(
        `select count(*) as count from "incomingWebhooks" where state = $(state) and "createdAt" < now() - interval '1 day'`,
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

    let triggered = 0
    for (const webhook of webhooks) {
      await emitter.triggerWebhookProcessing(webhook.platform, webhook.id)
      triggered++

      if (triggered % 100 === 0) {
        ctx.log.info(`Re-triggered ${triggered} webhooks!`)
      }
    }

    ctx.log.info(`Re-triggered ${triggered} stuck pending webhooks in total!`)
  },
}

async function getMessageCounts(
  log: Logger,
  admin: KafkaAdmin,
  topic: string,
  groupId: string,
): Promise<{ total: number; consumed: number; unconsumed: number }> {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(topic)
    const offsetsResponse = await admin.fetchOffsets({ groupId, topics: [topic] })
    const offsets = offsetsResponse[0].partitions

    let totalMessages = 0
    let consumedMessages = 0
    let totalLeft = 0

    for (const offset of offsets) {
      const topicOffset = topicOffsets.find((p) => p.partition === offset.partition)
      if (topicOffset) {
        totalMessages += Number(topicOffset.offset)
        consumedMessages += Number(offset.offset)
        totalLeft += Number(topicOffset.offset) - Number(offset.offset)
      }
    }

    return { total: totalMessages, consumed: consumedMessages, unconsumed: totalLeft }
  } catch (err) {
    log.error(err, 'Failed to get message count!')
    throw err
  }
}

export default job
