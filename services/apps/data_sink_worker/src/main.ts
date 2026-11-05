import { DataSinkWorkerEmitter, SearchSyncWorkerEmitter } from '@crowd/common_services'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'
import { getRedisClient } from '@crowd/redis'
import { Client as TemporalClient, getTemporalClient } from '@crowd/temporal'

import {
  DB_CONFIG,
  DB_TIMEOUTS_CONFIG,
  QUEUE_CONFIG,
  REDIS_CONFIG,
  TEMPORAL_CONFIG,
  WORKER_SETTINGS,
} from './conf'
import { WorkerQueueReceiver } from './queue'

const log = getServiceLogger()

const MAX_CONCURRENT_PROCESSING = 5

setImmediate(async () => {
  log.info('Starting data sink worker...')

  let temporal: TemporalClient | undefined
  // temp for production
  if (TEMPORAL_CONFIG().serverUrl) {
    temporal = await getTemporalClient(TEMPORAL_CONFIG())
  }

  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())

  const dbConnection = await getDbConnection(
    { ...DB_CONFIG(), ...DB_TIMEOUTS_CONFIG() },
    MAX_CONCURRENT_PROCESSING,
  )

  const redisClient = await getRedisClient(REDIS_CONFIG())

  const searchSyncWorkerEmitter = new SearchSyncWorkerEmitter(queueClient, log)

  const dataWorkerEmitter = new DataSinkWorkerEmitter(queueClient, log)

  const queue = new WorkerQueueReceiver(
    WORKER_SETTINGS().queuePriorityLevel,
    queueClient,
    dbConnection,
    searchSyncWorkerEmitter,
    dataWorkerEmitter,
    redisClient,
    temporal,
    log,
  )

  try {
    await searchSyncWorkerEmitter.init()
    await dataWorkerEmitter.init()

    await queue.start()
  } catch (err) {
    log.error({ err }, 'Failed to start queues!')
    process.exit(1)
  }
})
