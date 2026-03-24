/**
 * Transformer consumer: Infinite loop that polls DB → transforms.
 *
 * Continuously polls the metadata store for pending jobs
 * that need transformation, then runs the appropriate transformer.
 */
import { DataSinkWorkerEmitter } from '@crowd/common_services'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { QUEUE_CONFIG, QueueFactory } from '@crowd/queue'
import { REDIS_CONFIG, RedisCache, getRedisClient } from '@crowd/redis'
import { PlatformType } from '@crowd/types'

import { IntegrationResolver } from '../core/integrationResolver'
import { MetadataStore, SnowflakeExportJob } from '../core/metadataStore'
import { S3Service } from '../core/s3Service'
import { getDataSource, getEnabledPlatforms } from '../integrations'

const log = getServiceChildLogger('transformerConsumer')

const MAX_POLLING_INTERVAL_MS = 30 * 60 * 1000 // 30 minutes

export class TransformerConsumer {
  private running = false
  private currentPollingIntervalMs: number

  constructor(
    private readonly metadataStore: MetadataStore,
    private readonly s3Service: S3Service,
    private readonly integrationResolver: IntegrationResolver,
    private readonly emitter: DataSinkWorkerEmitter,
    private readonly pollingIntervalMs: number,
  ) {
    this.currentPollingIntervalMs = pollingIntervalMs
  }

  async start(): Promise<void> {
    this.running = true
    log.info('Transformer consumer started')

    while (this.running) {
      try {
        const job = await this.metadataStore.claimOldestPendingJob()
        log.info('Claiming job from metadata store', { job })

        if (job) {
          log.info({ jobId: job.id, platform: job.platform, s3Path: job.s3Path }, 'Processing job')
          this.currentPollingIntervalMs = this.pollingIntervalMs
          await this.processJob(job)
          continue
        }
      } catch (err) {
        log.error({ err }, 'Error in consumer loop')
        await this.sleep(this.pollingIntervalMs)
        continue
      }

      log.info(
        { currentPollingIntervalMs: this.currentPollingIntervalMs },
        'No pending jobs, backing off',
      )
      await this.sleep(this.currentPollingIntervalMs)
      this.currentPollingIntervalMs = Math.min(
        this.currentPollingIntervalMs * 2,
        MAX_POLLING_INTERVAL_MS,
      )
    }

    log.info('Transformer consumer stopped')
  }

  stop(): void {
    this.running = false
  }

  private async processJob(job: SnowflakeExportJob): Promise<void> {
    log.info({ jobId: job.id, platform: job.platform, s3Path: job.s3Path }, 'Processing job')

    const startTime = Date.now()

    try {
      const platform = job.platform as PlatformType
      const source = getDataSource(platform, job.sourceName)

      const rows = await this.s3Service.readParquetRows(job.s3Path)

      let transformedCount = 0
      let transformSkippedCount = 0
      let resolveSkippedCount = 0

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i]
        const result = source.transformer.safeTransformRow(row)
        if (!result) {
          transformSkippedCount++
          continue
        }

        const resolved = await this.integrationResolver.resolve(platform, result.segment)
        if (!resolved) {
          resolveSkippedCount++
          continue
        }

        await this.emitter.createAndProcessActivityResult(
          resolved.segmentId,
          resolved.integrationId,
          result.activity,
        )
        transformedCount++
      }

      const skippedCount = transformSkippedCount + resolveSkippedCount
      const processingMetrics = {
        transformedCount,
        skippedCount,
        transformSkippedCount,
        resolveSkippedCount,
        processingDurationMs: Date.now() - startTime,
      }

      await this.metadataStore.markCompleted(job.id, processingMetrics)

      log.info({ jobId: job.id, ...processingMetrics }, 'Job completed')
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err)
      log.error({ jobId: job.id, err }, 'Job failed')

      try {
        await this.metadataStore.markFailed(job.id, errorMessage, {
          processingDurationMs: Date.now() - startTime,
        })
      } catch (updateErr) {
        log.error({ jobId: job.id, updateErr }, 'Failed to mark job as failed')
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }
}

export async function createTransformerConsumer(): Promise<TransformerConsumer> {
  const db = await getDbConnection(WRITE_DB_CONFIG())
  const metadataStore = new MetadataStore(db)
  const s3Service = new S3Service()
  const redisClient = await getRedisClient(REDIS_CONFIG(), true)
  const cache = new RedisCache('snowflake-integration-resolver', redisClient, log)
  const resolver = new IntegrationResolver(db, cache)
  await resolver.preloadPlatforms(getEnabledPlatforms())

  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())
  const emitter = new DataSinkWorkerEmitter(queueClient, log)
  await emitter.init()

  const pollingIntervalMs = 10_000 // 10 seconds

  return new TransformerConsumer(metadataStore, s3Service, resolver, emitter, pollingIntervalMs)
}
