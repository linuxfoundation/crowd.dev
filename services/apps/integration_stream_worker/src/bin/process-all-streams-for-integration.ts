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
import { IntegrationStreamState } from '@crowd/types'

import { DB_CONFIG, QUEUE_CONFIG, REDIS_CONFIG } from '../conf'
import IntegrationStreamService from '../service/integrationStreamService'

const BATCH_SIZE = 100
const MAX_CONCURRENT = 1

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

if (processArguments.length !== 1) {
  log.error('Expected 1 argument: integrationId')
  process.exit(1)
}

const integrationId = processArguments[0]

async function processStream(
  streamId: string,
  service: IntegrationStreamService,
): Promise<boolean> {
  try {
    return await service.processStream(streamId)
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
    from integration.streams
    where state in ('${IntegrationStreamState.ERROR}', '${IntegrationStreamState.PENDING}')
    and "integrationId" = '${integrationId}'
    order by id
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
      const streamId = result.id

      log.info(`Processing stream ${streamId}!`)

      current++
      processStream(streamId, service).then((res) => {
        current--
        if (res) {
          total++
        } else {
          errors++
        }

        log.info({ res }, `Processed ${total} streams successfully so far! ${errors} errors!`)
      })
    }

    results = await dbConnection.any(
      `
      select id
      from integration.streams
      where state in ('${IntegrationStreamState.ERROR}', '${IntegrationStreamState.PENDING}')
      and id > $(lastId)
      and "integrationId" = '${integrationId}'
      order by id
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
