import { BatchProcessor, generateUUIDv1 } from '@crowd/common'
import { DataSinkWorkerEmitter, SearchSyncWorkerEmitter } from '@crowd/common_services'
import { DbConnection, DbStore } from '@crowd/data-access-layer/src/database'
import { IResultData } from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/dataSink.data'
import { Logger, getChildLogger, logExecutionTimeV2 } from '@crowd/logging'
import { CrowdQueue, IQueue, PrioritizedQueueReciever } from '@crowd/queue'
import { RedisClient } from '@crowd/redis'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  CreateAndProcessActivityResultQueueMessage,
  DataSinkWorkerQueueMessageType,
  IActivityData,
  IQueueMessage,
  ProcessIntegrationResultQueueMessage,
  QueuePriorityLevel,
} from '@crowd/types'

import DataSinkService from '../service/dataSink.service'

export class WorkerQueueReceiver extends PrioritizedQueueReciever {
  private readonly batchProcessor: BatchProcessor<{
    resultId: string
    data: IResultData | undefined
    created: boolean
  }>

  private readonly batchPreprocessor: BatchProcessor<{
    segmentId: string
    integrationId: string
    data: IActivityData
    resultId?: string
  }>

  constructor(
    level: QueuePriorityLevel,
    private readonly client: IQueue,
    private readonly pgConn: DbConnection,
    private readonly searchSyncWorkerEmitter: SearchSyncWorkerEmitter,
    private readonly dataSinkWorkerEmitter: DataSinkWorkerEmitter,
    private readonly redisClient: RedisClient,
    private readonly temporal: TemporalClient,
    parentLog: Logger,
  ) {
    super(
      level,
      client,
      client.getQueueChannelConfig(CrowdQueue.DATA_SINK_WORKER),
      Number(process.env.WORKER_MAX_CONCURRENCY || 1),
      parentLog,
      undefined,
      undefined,
      10,
    )

    this.batchProcessor = new BatchProcessor<{
      resultId: string
      data: IResultData | undefined
      created: boolean
    }>(
      Number(process.env.BATCH_PROCESSOR_SIZE || 20),
      30,
      async (batch) => {
        const batchId = generateUUIDv1()
        const logger = getChildLogger('processBatchIntegrationResults', this.log, {
          batchSize: batch.length,
          batchId,
        })

        const service = new DataSinkService(
          new DbStore(logger, this.pgConn, undefined, false),
          this.searchSyncWorkerEmitter,
          this.dataSinkWorkerEmitter,
          this.redisClient,
          this.temporal,
          this.client,
          logger,
        )

        await logExecutionTimeV2(
          async () => service.processResults(batch),
          logger,
          'dataSinkService -> processResults',
        )
      },
      async (_, err) => {
        this.log.error(err, 'Error while processing batch!')
        throw err
      },
    )

    this.batchPreprocessor = new BatchProcessor<{
      segmentId: string
      integrationId: string
      data: IActivityData
      resultId?: string
    }>(
      10,
      30,
      async (batch) => {
        const batchId = generateUUIDv1()
        const logger = getChildLogger('preProcessBatchIntegrationResults', this.log, {
          batchSize: batch.length,
          batchId,
        })

        const service = new DataSinkService(
          new DbStore(logger, this.pgConn, undefined, false),
          this.searchSyncWorkerEmitter,
          this.dataSinkWorkerEmitter,
          this.redisClient,
          this.temporal,
          this.client,
          logger,
        )

        const results = await logExecutionTimeV2(
          async () => service.prepareInMemoryActivityResults(batch),
          logger,
          'dataSinkService -> prepareInMemoryActivityResults',
        )

        for (const result of results) {
          await this.batchProcessor.addToBatch(result)
        }
      },
      async (_, err) => {
        this.log.error(err, 'Error while processing batch!')
        throw err
      },
    )
  }

  override async stop(): Promise<void> {
    super.stop()
    await this.batchPreprocessor.stop()
    await this.batchProcessor.stop()
  }

  override async processMessage(message: IQueueMessage): Promise<void> {
    try {
      this.log.trace({ messageType: message.type }, 'Processing message!')

      switch (message.type) {
        case DataSinkWorkerQueueMessageType.PROCESS_INTEGRATION_RESULT: {
          const resultId = (message as ProcessIntegrationResultQueueMessage).resultId
          await this.batchProcessor.addToBatch({ resultId, data: undefined, created: true })
          break
        }

        case DataSinkWorkerQueueMessageType.CREATE_AND_PROCESS_ACTIVITY_RESULT: {
          const msg = message as CreateAndProcessActivityResultQueueMessage

          await this.batchPreprocessor.addToBatch({
            segmentId: msg.segmentId,
            integrationId: msg.integrationId,
            data: msg.activityData,
            resultId: msg.resultId,
          })

          break
        }

        case DataSinkWorkerQueueMessageType.CHECK_RESULTS: {
          const service = new DataSinkService(
            new DbStore(this.log, this.pgConn, undefined, false),
            this.searchSyncWorkerEmitter,
            this.dataSinkWorkerEmitter,
            this.redisClient,
            this.temporal,
            this.client,
            this.log,
          )

          await service.checkResults()
          break
        }

        default:
          throw new Error(`Unknown message type: ${message.type}`)
      }
    } catch (err) {
      this.log.error(err, 'Error while processing message!')
      throw err
    }
  }
}
