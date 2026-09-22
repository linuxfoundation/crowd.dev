import { timeout } from '@crowd/common'
import {
  DataSinkWorkerEmitter,
  IntegrationRunWorkerEmitter,
  IntegrationStreamWorkerEmitter,
} from '@crowd/common_services'
import { DbStore, getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'
import { getRedisClient } from '@crowd/redis'
import { WebhookType } from '@crowd/types'

import { DB_CONFIG, QUEUE_CONFIG, REDIS_CONFIG } from '../conf'
import IntegrationStreamService from '../service/integrationStreamService'

const BATCH_SIZE = 100
const MAX_CONCURRENT = 3

const log = getServiceLogger()

async function processWebhook(
  webhookId: string,
  service: IntegrationStreamService,
): Promise<boolean> {
  try {
    return await service.processWebhookStream(webhookId)
  } catch {
    return false
  }
}

setImmediate(async () => {
  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())
  const dbConnection = await getDbConnection(DB_CONFIG())
  const store = new DbStore(log, dbConnection)

  const redisClient = await getRedisClient(REDIS_CONFIG(), true)
  const runWorkerEmiiter = new IntegrationRunWorkerEmitter(queueClient, log)
  const streamWorkerEmitter = new IntegrationStreamWorkerEmitter(queueClient, log)
  const dataSinkWorkerEmitter = new DataSinkWorkerEmitter(queueClient, log)

  await runWorkerEmiiter.init()
  await streamWorkerEmitter.init()
  await dataSinkWorkerEmitter.init()

  const service = new IntegrationStreamService(
    redisClient,
    runWorkerEmiiter,
    streamWorkerEmitter,
    dataSinkWorkerEmitter,
    store,
    log,
  )

  let results = await dbConnection.any(
    `
    select id
    from "incomingWebhooks"
    where state in ('PENDING', 'ERROR') and type not in ('${WebhookType.DISCOURSE}', '${WebhookType.CROWD_GENERATED}')
    order by 
    case when state = 'PENDING' then 1
    else 2
    end,
    id
    limit ${BATCH_SIZE};
    `,
  )

  let current = 0
  let total = 0
  let errors = 0
  while (results.length > 0) {
    for (const result of results) {
      while (current == MAX_CONCURRENT) {
        await timeout(1000)
      }
      const webhookId = result.id

      current++
      processWebhook(webhookId, service).then((res) => {
        current--

        if (res) {
          total++
        } else {
          errors++
        }

        log.info({ res }, `Processed ${total} webhooks successfully so far! ${errors} errors!`)
      })
    }

    results = await dbConnection.any(
      `
      select id
      from "incomingWebhooks"
      where state in ('PENDING', 'ERROR') and type not in ('${WebhookType.DISCOURSE}', '${WebhookType.CROWD_GENERATED}')
      and id > $(lastId)
      order by
      case when state = 'PENDING' then 1
      else 2
      end,
      id
      limit ${BATCH_SIZE};
      `,
      {
        lastId: results[results.length - 1].id,
      },
    )
  }

  while (current > 0) {
    await timeout(1000)
  }

  process.exit(0)
})
