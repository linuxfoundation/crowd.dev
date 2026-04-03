import CronTime from 'cron-time-generator'

import { IS_PROD_ENV, generateUUIDv1, partition } from '@crowd/common'
import { DataSinkWorkerEmitter } from '@crowd/common_services'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { Logger } from '@crowd/logging'
import { KafkaAdmin, QUEUE_CONFIG, getKafkaClient } from '@crowd/queue'
import { KafkaQueueService } from '@crowd/queue/src/vendors/kafka/client'
import { DataSinkWorkerQueueMessageType, IntegrationResultState } from '@crowd/types'

import { IJobDefinition } from '../types'

const job: IJobDefinition = {
  name: 'integration-results-check',
  cronTime: CronTime.every(10).minutes(),
  timeout: 30 * 60, // 30 minutes
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    const topic = 'data-sink-worker-normal-production'
    const groupId = 'data-sink-worker-normal-production'

    const kafkaClient = getKafkaClient(QUEUE_CONFIG())
    const admin = kafkaClient.admin()
    await admin.connect()

    const counts = await getMessageCounts(ctx.log, admin, topic, groupId)

    // if we have less than 50k messages in the queue we can trigger 50k oldest results (we process between 100k and 300k results per hour on average)
    if (counts.unconsumed < 50000) {
      const dbConnection = await getDbConnection(WRITE_DB_CONFIG())

      // we check if we have more than unconsumed pending results so that we don't trigger just the ones in the queue :)
      const count = (
        await dbConnection.one(
          `select count(*) as count from integration.results where state = '${IntegrationResultState.PENDING}'`,
        )
      ).count

      if (count > counts.unconsumed) {
        ctx.log.info(`We have ${count} pending results, triggering 100k oldest results!`)

        const queueService = new KafkaQueueService(kafkaClient, ctx.log)
        const dswEmitter = new DataSinkWorkerEmitter(queueService, ctx.log)
        await dswEmitter.init()

        const resultIds = (
          await dbConnection.any(
            `select id from integration.results where state = 'pending' order by "createdAt" desc limit 100000`,
          )
        ).map((r) => r.id)

        let triggered = 0

        for (const batch of partition(resultIds, 10)) {
          const messages = batch.map((resultId) => {
            return {
              payload: {
                type: DataSinkWorkerQueueMessageType.PROCESS_INTEGRATION_RESULT,
                resultId,
              },
              groupId: generateUUIDv1(),
              deduplicationId: resultId,
            }
          })

          await dswEmitter.sendMessages(messages)

          triggered += batch.length

          if (triggered % 1000 === 0) {
            ctx.log.info(`Triggered ${triggered} results!`)
          }
        }

        ctx.log.info(`Triggered ${triggered} results in total!`)
      }
    } else {
      ctx.log.info(`We have ${counts.unconsumed} unconsumed messages in the queue, skipping!`)
    }
  },
}

async function getMessageCounts(
  log: Logger,
  admin: KafkaAdmin,
  topic: string,
  groupId: string,
): Promise<{
  total: number
  consumed: number
  unconsumed: number
}> {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(topic)
    const offsetsResponse = await admin.fetchOffsets({
      groupId: groupId,
      topics: [topic],
    })

    const offsets = offsetsResponse[0].partitions

    let totalMessages = 0
    let consumedMessages = 0
    let totalLeft = 0

    for (const offset of offsets) {
      const topicOffset = topicOffsets.find((p) => p.partition === offset.partition)
      if (topicOffset) {
        // Total messages is the latest offset
        totalMessages += Number(topicOffset.offset)
        // Consumed messages is the consumer group's offset
        consumedMessages += Number(offset.offset)
        // Unconsumed is the difference
        totalLeft += Number(topicOffset.offset) - Number(offset.offset)
      }
    }

    return {
      total: totalMessages,
      consumed: consumedMessages,
      unconsumed: totalLeft,
    }
  } catch (err) {
    log.error(err, 'Failed to get message count!')
    throw err
  }
}

export default job
