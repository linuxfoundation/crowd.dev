import {
  addSeconds,
  generateUUIDv1,
  groupBy,
  single,
  singleOrDefault,
  timeout,
} from '@crowd/common'
import { ApplicationError, UnrepeatableError } from '@crowd/common'
import { DataSinkWorkerEmitter, SearchSyncWorkerEmitter } from '@crowd/common_services'
import { DbStore } from '@crowd/data-access-layer/src/database'
import {
  IIntegrationData,
  IResultData,
} from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/dataSink.data'
import DataSinkRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/dataSink.repo'
import { Logger, LoggerBase, logExecutionTimeV2 } from '@crowd/logging'
import { IQueue } from '@crowd/queue'
import { RedisClient } from '@crowd/redis'
import telemetry from '@crowd/telemetry'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  IActivityData,
  IIntegrationResult,
  IMemberData,
  IntegrationResultState,
  IntegrationResultType,
  PlatformType,
} from '@crowd/types'

import { WORKER_SETTINGS } from '../conf'

import ActivityService from './activity.service'
import MemberService from './member.service'

/* eslint-disable @typescript-eslint/no-explicit-any */

export default class DataSinkService extends LoggerBase {
  private readonly repo: DataSinkRepository

  constructor(
    private readonly pgStore: DbStore,
    private readonly searchSyncWorkerEmitter: SearchSyncWorkerEmitter,
    private readonly dataSinkWorkerEmitter: DataSinkWorkerEmitter,
    private readonly redisClient: RedisClient,
    private readonly temporal: TemporalClient,
    private readonly client: IQueue,
    parentLog: Logger,
  ) {
    super(parentLog)

    this.repo = new DataSinkRepository(pgStore, this.log)
  }

  private async triggerResultError(
    resultInfo: IResultData,
    resultExists: boolean,
    location: string,
    message: string,
    error: any,
    metadata?: Record<string, unknown>,
  ): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const errorData: any = {
      location,
      message,

      metadata,

      errorMessage: error?.message ?? '<no error message>',

      errors: [
        {
          errorMessage: error?.message ?? '<no error message>',
          errorStack: error?.stack,
          metadata: error?.metadata,
          error,
        },
      ],
    }

    // unwrap all errors
    let currentError = error
    while (currentError instanceof ApplicationError && currentError.originalError) {
      currentError = currentError.originalError

      errorData.errors.push({
        errorMessage: currentError.message,
        errorStack: currentError.stack,
        metadata: currentError.metadata,
        error: currentError,
      })

      // chain to the  topmost errorMessage property
      errorData.errorMessage = `${errorData.errorMessage} -> ${
        currentError.message ?? '<no error message>'
      }`
    }

    if (errorData.errorMessage.includes('uix_memberIdentities_platform_value_type_verified')) {
      const delaySeconds = Math.floor(Math.random() * (120 - 10 + 1) + 10) * 60
      const until = addSeconds(new Date(), delaySeconds)
      this.log.warn(
        { until: until.toISOString(), delayMinutes: delaySeconds / 60 },
        'Delaying result due to member identity uniqueness conflict!',
      )
      await this.repo.delayResult(
        resultInfo.id,
        until,
        errorData,
        resultExists ? undefined : resultInfo,
      )
      return
    }

    if (
      !(error instanceof UnrepeatableError) &&
      resultInfo.retries + 1 <= WORKER_SETTINGS().maxStreamRetries
    ) {
      // delay for #retries * 2 minutes
      const until = addSeconds(new Date(), (resultInfo.retries + 1) * 2 * 60)
      this.log.warn({ until: until.toISOString() }, 'Retrying result!')

      await this.repo.delayResult(
        resultInfo.id,
        until,
        errorData,
        resultExists ? undefined : resultInfo,
      )
    } else {
      await this.repo.markResultError(
        resultInfo.id,
        errorData,
        resultExists ? undefined : resultInfo,
      )
    }
  }

  public async checkResults(): Promise<void> {
    this.log.info('Checking for delayed results!')

    let results = await this.repo.transactionally(async (txRepo) => {
      return await txRepo.getDelayedResults(10)
    })

    while (results.length > 0) {
      this.log.info({ count: results.length }, 'Found delayed results!')

      for (const result of results) {
        this.log.info({ resultId: result.id }, 'Restarting delayed stream!')
        await this.repo.resetResults([result.id])
        await this.dataSinkWorkerEmitter.triggerResultProcessing(
          result.id,
          result.id,
          result.onboarding === null ? true : result.onboarding,
          `${result.id}-delayed-${Date.now()}`,
        )
      }

      results = await this.repo.transactionally(async (txRepo) => {
        return await txRepo.getDelayedResults(10)
      })
    }
  }

  public async prepareInMemoryActivityResults(
    results: { segmentId: string; integrationId: string; data: IActivityData; resultId?: string }[],
  ): Promise<{ resultId: string; data: IResultData; created: boolean }[]> {
    const integrationIds = results.map((r) => r.integrationId)
    const integrations = await this.repo.getIntegrationInfos(integrationIds)

    const prepared: { resultId: string; data: IResultData; created: boolean }[] = []

    const promises = []

    for (const toProcess of results) {
      const { segmentId, integrationId, data, resultId } = toProcess
      const id = resultId ?? generateUUIDv1()

      const payload: IIntegrationResult = {
        type: IntegrationResultType.ACTIVITY,
        data,
        segmentId,
      }

      const result: IResultData = {
        id,
        integrationId,
        data: payload,
        state: IntegrationResultState.PENDING,
        runId: null,
        streamId: null,
        webhookId: null,
        platform: null,
        segmentId,
        retries: 0,
        delayedUntil: null,
        onboarding: false,
      }

      let integration: IIntegrationData | null = null

      if (integrationId) {
        integration = singleOrDefault(integrations, (i) => i.integrationId === integrationId)

        if (integration) {
          result.platform = integration.platform

          if (result.platform === PlatformType.GITHUB_NANGO) {
            result.platform = PlatformType.GITHUB
          }

          const platform = (payload.data as IActivityData).platform as PlatformType

          if (
            segmentId &&
            integration.segmentId !== segmentId &&
            ![PlatformType.GITHUB, PlatformType.GITLAB].includes(platform)
          ) {
            // save error and stop
            await logExecutionTimeV2(
              async () =>
                this.repo.markResultError(
                  id,
                  new Error(
                    `Segment id from queue message '${segmentId}' does not equal integration '${integration.integrationId}' segment id '${integration.segmentId}'!`,
                  ),
                  resultId === undefined ? result : undefined,
                ),
              this.log,
              'dataSinkService -> processActivityInMemoryResult -> markResultError',
            )
            return
          }

          if (!segmentId) {
            result.segmentId = integration.segmentId
          }
        }
      }

      // TODO remove when we have all integrations storing results on their own
      // create result in db first
      if (!resultId) {
        promises.push(this.repo.publishExternalResult(id, integration.integrationId, payload))
      }

      prepared.push({
        resultId: id,
        data: result,
        created: true,
      })
    }

    await Promise.all(promises)

    this.log.info(
      `[RESULTS] Prepared ${prepared.length} in memory results stored ${promises.length} in db!`,
    )

    return prepared
  }

  public async processResults(
    resultsToProcess: { resultId: string; data: IResultData | undefined; created: boolean }[],
    postProcess = true,
  ): Promise<void> {
    const batch: { resultId: string; data: IResultData | undefined; created: boolean }[] = []
    for (const result of resultsToProcess) {
      const filtered = resultsToProcess.filter((r) => r.resultId === result.resultId)

      // check if we already have this result in the batch
      if (!batch.some((b) => b.resultId === filtered[0].resultId)) {
        if (filtered.length > 1) {
          this.log.warn(
            { resultId: result.resultId },
            'Found multiple results for the same result id!',
          )
        }

        batch.push(filtered[0])
      }
    }

    this.log.trace(`[RESULTS] Processing ${batch.length} results!`)
    const start = performance.now()

    const results: IResultData[] = []

    const toLoadById: string[] = []
    for (const toProcess of batch) {
      const { resultId, data } = toProcess

      if (!data) {
        toLoadById.push(resultId)
      } else {
        results.push(data)
      }
    }

    if (toLoadById.length > 0) {
      this.log.info(`[RESULTS] Loading ${toLoadById.length} results from db!`)
      const infos = await this.repo.getResultInfos(toLoadById)
      results.push(...infos)
    }

    const groupedByType = groupBy(results, (r) => r.data.type)

    const resultMap = new Map<
      string,
      { success: boolean; err?: any; metadata?: Record<string, unknown> }
    >()
    const types = Array.from(groupedByType.keys()) as IntegrationResultType[]
    for (const type of types) {
      if (type === IntegrationResultType.ACTIVITY) {
        const results = groupedByType.get(type)

        for (const result of results) {
          if (!result.platform) {
            result.platform = (result.data.data as IActivityData).platform as PlatformType
          }

          if (result.platform === PlatformType.GITHUB_NANGO) {
            result.platform = PlatformType.GITHUB
          }
        }

        const service = new ActivityService(
          this.pgStore,
          this.searchSyncWorkerEmitter,
          this.redisClient,
          this.temporal,
          this.client,
          this.log,
        )

        let retry = 0
        let toProcess = results

        let lastError: Error | undefined

        let lastResults:
          | Map<string, { success: boolean; err?: Error; metadata?: Record<string, unknown> }>
          | undefined = undefined

        while (toProcess.length > 0 && retry < 5) {
          if (retry > 0) {
            this.log.trace(`[RESULTS] Retrying but sleeping first...`)
            await timeout(100)
          }

          try {
            this.log.trace(`[RESULTS] Processing ${toProcess.length} activity results...`)
            lastResults = await service.processActivities(
              toProcess.map((r) => {
                return {
                  resultId: r.id,
                  integrationId: r.integrationId,
                  onboarding: r.onboarding === null ? true : r.onboarding,
                  platform: r.platform,
                  activity: r.data.data as IActivityData,
                  segmentId: r.data.segmentId ?? r.segmentId,
                }
              }),
              toProcess.some((r) => r.onboarding === true),
            )

            toProcess = []
            for (const [resultId, result] of lastResults) {
              if (result.success || result.err instanceof UnrepeatableError || result.metadata) {
                resultMap.set(resultId, result)
              } else {
                toProcess.push(single(results, (r) => r.id === resultId))
              }
            }

            this.log.trace(
              `[RESULTS] Processed ${lastResults.size} activity results! We have total of ${resultMap.size} results for this batch and ${toProcess.length} to retry.`,
            )

            // clear last error because we processed without unhandled error
            lastError = undefined
          } catch (err) {
            // will be retried
            this.log.error(err, 'Unhandled error while processing activities!')
            // save last error so in case we hit retry limit we can set it for all results left to process
            lastError = err
          }

          retry++
        }

        // if lastError is still set and we have some left to process, we set the error for them cuz they were retried but failed
        if (lastError && toProcess.length > 0) {
          this.log.trace(
            `[RESULTS] Setting error for ${toProcess.length} activity results because we hit a retry limit!`,
          )
          for (const leftToProcess of toProcess) {
            resultMap.set(leftToProcess.id, {
              success: false,
              err: lastError,
            })
          }
        }

        if (lastResults) {
          for (const left of toProcess) {
            if (resultMap.has(left.id)) {
              continue
            }

            const lastResult = lastResults.get(left.id)
            if (lastResult) {
              resultMap.set(left.id, lastResult)
            }
          }
        }
      } else if (type === IntegrationResultType.TWITTER_MEMBER_REACH) {
        // just process individually
        for (const entry of groupedByType.get(type)) {
          try {
            const service = new MemberService(
              this.pgStore,
              this.redisClient,
              this.temporal,
              this.log,
            )
            const memberData = entry.data.data as IMemberData

            await service.processMemberUpdate(entry.integrationId, entry.platform, memberData)
            resultMap.set(entry.id, { success: true })
          } catch (err) {
            resultMap.set(entry.id, { success: false, err })
          }
        }
      } else {
        this.log.error(`[RESULTS] Unknown result type: ${type}!`)
      }
    }

    if (!postProcess) {
      this.log.info(`[RESULTS] Skipping post processing!`)
      return
    }

    this.log.trace(`[RESULTS] Processing ${resultMap.size} process results!`)

    // handle results
    let errors = 0
    let successes = 0
    let deletions = 0
    const resultsToDelete: string[] = []
    for (const [resultId, result] of resultMap) {
      const batchEntry = single(batch, (b) => b.resultId === resultId)

      if (!result.success) {
        const resultInfo = single(results, (r) => r.id === resultId)

        await this.triggerResultError(
          resultInfo,
          batchEntry.created,
          'process-result',
          'Error processing result.',
          result.err,
          result.metadata,
        )

        errors++
      } else {
        successes++
        if (batchEntry.created) {
          deletions++
          resultsToDelete.push(resultId)
        }
      }
    }

    if (resultsToDelete.length > 0 && postProcess) {
      this.log.trace(`[RESULTS] Deleting ${resultsToDelete.length} results from db!`)

      await this.repo.deleteResults(resultsToDelete)
    }

    this.log.trace(
      `Processed ${successes} results successfully, ${errors} with error, ${deletions} were deleted from db!`,
    )

    const end = performance.now()
    const totalTime = end - start

    for (const type of types) {
      const items = groupedByType.get(type)
      const msPerItem = Math.floor(totalTime / items.length)

      const args = { type }

      if (type === IntegrationResultType.ACTIVITY) {
        items.forEach((item) => {
          const activityArgs = {
            ...args,
            platform: item.platform,
            integrationId: item.integrationId,
            onboarding:
              item.onboarding === null || item.onboarding === undefined
                ? '<not-set>'
                : item.onboarding.toString(),
            channel: (item.data.data as IActivityData).channel,
          }
          telemetry.distribution('data_sink_worker.process_result', msPerItem, activityArgs)
        })
      } else {
        items.forEach(() => {
          telemetry.distribution('data_sink_worker.process_result', msPerItem, args)
        })
      }
    }
  }
}
