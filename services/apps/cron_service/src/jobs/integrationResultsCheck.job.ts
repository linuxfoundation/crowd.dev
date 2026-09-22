import CronTime from 'cron-time-generator'

import { IS_PROD_ENV, generateUUIDv1, partition } from '@crowd/common'
import { DataSinkWorkerEmitter } from '@crowd/common_services'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { QUEUE_CONFIG, getKafkaClient, getKafkaMessageCounts } from '@crowd/queue'
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

    try {
      const counts = await getKafkaMessageCounts(ctx.log, admin, topic, groupId)

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
    } finally {
      await admin.disconnect()
    }
  },
}

export default job
