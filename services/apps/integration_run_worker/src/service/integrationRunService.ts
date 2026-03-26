import { singleOrDefault } from '@crowd/common'
import {
  IntegrationRunWorkerEmitter,
  IntegrationStreamWorkerEmitter,
  SearchSyncWorkerEmitter,
} from '@crowd/common_services'
import { DbStore } from '@crowd/data-access-layer/src/database'
import IntegrationRunRepository from '@crowd/data-access-layer/src/old/apps/integration_run_worker/integrationRun.repo'
import MemberAttributeSettingsRepository from '@crowd/data-access-layer/src/old/apps/integration_run_worker/memberAttributeSettings.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { populateGithubSettingsWithRepos } from '@crowd/data-access-layer/src/repositories'
import { IGenerateStreamsContext, INTEGRATION_SERVICES } from '@crowd/integrations'
import { Logger, LoggerBase, getChildLogger } from '@crowd/logging'
import { ApiPubSubEmitter, RedisCache, RedisClient } from '@crowd/redis'
import { IntegrationRunState, IntegrationStreamState, PlatformType } from '@crowd/types'

import { NANGO_CONFIG, PLATFORM_CONFIG, WORKER_CONFIG } from '../conf'

export default class IntegrationRunService extends LoggerBase {
  private readonly repo: IntegrationRunRepository

  constructor(
    private readonly redisClient: RedisClient,
    private readonly streamWorkerEmitter: IntegrationStreamWorkerEmitter,
    private readonly runWorkerEmitter: IntegrationRunWorkerEmitter,
    private readonly searchSyncWorkerEmitter: SearchSyncWorkerEmitter,
    private readonly apiPubSubEmitter: ApiPubSubEmitter,
    private readonly store: DbStore,
    parentLog: Logger,
  ) {
    super(parentLog)

    this.repo = new IntegrationRunRepository(store, this.log)
  }

  public async handleStreamProcessed(runId: string): Promise<void> {
    this.log = getChildLogger('stream-processed', this.log, {
      runId,
    })

    this.log.debug('Checking whether run is processed or not!')

    const counts = await this.repo.getStreamCountsByState(runId)

    let count = 0
    let finishedCount = 0
    let error = false
    for (const [state, stateCount] of counts.entries()) {
      count += stateCount

      if (state === IntegrationStreamState.ERROR) {
        finishedCount += stateCount
        error = true
      } else if (state === IntegrationStreamState.PROCESSED) {
        finishedCount += stateCount
      }
    }

    if (count === finishedCount) {
      const runInfo = await this.repo.getGenerateStreamData(runId)

      if (error) {
        this.log.warn('Some streams have resulted in error!')

        const pendingRetry = await this.repo.getErrorStreamsPendingRetry(
          runId,
          WORKER_CONFIG().maxRetries,
        )
        if (pendingRetry === 0) {
          this.log.error('No streams pending retry and all are in final state - run failed!')
          await this.repo.markRunError(runId, {
            location: 'all-streams-processed',
            message: 'Some streams failed!',
          })

          if (runInfo.onboarding) {
            this.log.warn('Onboarding - marking integration as failed!')
            await this.repo.markIntegration(runId, 'error')

            this.apiPubSubEmitter.emitIntegrationCompleted(runInfo.integrationId, 'error')
          } else {
            const last5RunStates = await this.repo.getLastRuns(runId, 3)
            if (
              last5RunStates.length === 3 &&
              last5RunStates.find((s) => s !== IntegrationRunState.ERROR) === undefined
            ) {
              this.log.warn(
                'Last 3 runs have all failed and now this one has failed - marking integration as failed!',
              )
              await this.repo.markIntegration(runId, 'error')
            }
          }
        } else {
          this.log.debug('Some streams are pending retry - run is not finished yet!')
        }
      } else {
        this.log.info('Run finished successfully!')

        try {
          this.log.info('Trying to post process integration settings!')

          const service = singleOrDefault(
            INTEGRATION_SERVICES,
            (s) => s.type === runInfo.integrationType,
          )

          if (service.postProcess) {
            let settings = runInfo.integrationSettings as object
            const newSettings = service.postProcess(settings)

            if (newSettings) {
              settings = { ...settings, ...newSettings }
              await this.updateIntegrationSettings(runId, settings)
            }

            this.log.info('Finished post processing integration settings!')
          } else {
            this.log.info('Integration does not have post processing!')
          }
        } catch (err) {
          this.log.error({ err }, 'Error while post processing integration settings!')
        }

        this.log.info('Marking run and integration as successfully processed!')
        await this.repo.markRunProcessed(runId)
        await this.repo.markIntegration(runId, 'done')

        if (runInfo.onboarding) {
          this.apiPubSubEmitter.emitIntegrationCompleted(runInfo.integrationId, 'done')
        }
      }
    }
  }

  public async checkRuns(): Promise<void> {
    this.log.debug('Checking for delayed runs!')

    let runs = await this.repo.getPendingDelayedRuns(1, 10)
    while (runs.length > 0) {
      this.log.info({ runCount: runs.length }, 'Found delayed runs that need to be started!')

      for (const run of runs) {
        this.log.info({ runId: run.id }, 'Restarting delayed run!')
        await this.repo.resetDelayedRun(run.id)
        await this.streamWorkerEmitter.continueProcessingRunStreams(
          run.onboarding,
          run.integrationType,
          run.id,
        )
      }

      runs = await this.repo.getPendingDelayedRuns(1, 10)
    }
  }

  public async startIntegrationRun(
    integrationId: string,
    onboarding: boolean,
    isManualRun?: boolean,
    manualSettings?: unknown,
    additionalInfo?: unknown,
  ): Promise<void> {
    this.log = getChildLogger('start-integration-run', this.log, {
      integrationId,
      onboarding,
    })

    this.log.info('Trying to start integration run!')

    const integrationInfo = await this.repo.getIntegrationData(integrationId)

    if (!integrationInfo) {
      this.log.error('Could not find integration info!')
      return
    }

    this.log = getChildLogger('start-integration-run', this.log, {
      integrationId,
      onboarding,
      integrationType: integrationInfo.type,
    })

    const service = singleOrDefault(INTEGRATION_SERVICES, (s) => s.type === integrationInfo.type)

    if (!service) {
      this.log.error('Integration type/platform not supported by this worker!')
      return
    }

    if (!onboarding) {
      const isAlreadyProcessing = await this.repo.isIntegrationBeingProcessed(integrationId)

      if (isAlreadyProcessing) {
        this.log.warn('Integration is already being processed!')
        return
      }
    }

    this.log.info('Creating new integration run!')
    const runId = await this.repo.createRun(integrationId, onboarding)

    this.log.info('Triggering run processing!')
    await this.runWorkerEmitter.triggerRunProcessing(
      integrationInfo.type,
      runId,
      onboarding,
      isManualRun,
      manualSettings,
      additionalInfo,
    )
  }

  public async generateStreams(
    runId: string,
    isManualRun?: boolean,
    manualSettings?: unknown,
    additionalInfo?: unknown,
  ): Promise<void> {
    this.log.info({ runId }, 'Trying to generate root streams for integration run!')

    const runInfo = await this.repo.getGenerateStreamData(runId)

    if (!runInfo) {
      this.log.error({ runId }, 'Could not find run info!')
      return
    }

    // Populate orgs[].repos from repositories table for github integrations
    if (
      runInfo.integrationType === PlatformType.GITHUB ||
      runInfo.integrationType === PlatformType.GITHUB_NANGO
    ) {
      runInfo.integrationSettings = await populateGithubSettingsWithRepos(
        dbStoreQx(this.store),
        runInfo.integrationId,
        runInfo.integrationSettings,
      )
    }

    // we can do this because this service instance is only used for one run
    this.log = getChildLogger('run-processor', this.log, {
      runId,
      integrationId: runInfo.integrationId,
      onboarding: runInfo.onboarding,
      platform: runInfo.integrationType,
    })

    if (runInfo.streamCount > 0) {
      this.log.error({ streamCount: runInfo.streamCount }, 'Run already has streams!')
      await this.triggerRunError(runId, 'check-run-streams', 'Run already has streams!', {
        streamCount: runInfo.streamCount,
      })
      return
    }

    if (runInfo.runState !== IntegrationRunState.PENDING) {
      this.log.error({ actualState: runInfo.runState }, 'Run is not in pending state!')
      await this.triggerRunError(runId, 'check-run-state', 'Run is not in pending state!', {
        actualState: runInfo.runState,
      })
      return
    }

    const integrationService = singleOrDefault(
      INTEGRATION_SERVICES,
      (i) => i.type === runInfo.integrationType,
    )

    if (!integrationService) {
      this.log.error({ type: runInfo.integrationType }, 'Could not find integration service!')
      await this.triggerRunError(
        runId,
        'check-run-int-service',
        'Could not find integration service!',
        {
          type: runInfo.integrationType,
        },
      )
      return
    }

    if (
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (runInfo.integrationSettings as any).updateMemberAttributes &&
      integrationService.memberAttributes.length > 0
    ) {
      this.log.warn('Integration settings contain updateMemberAttributes - updating it now!')
      await this.store.transactionally(async (txStore) => {
        const txMemberAttributeSettingsRepo = new MemberAttributeSettingsRepository(
          txStore,
          this.log,
        )
        const txRunRepo = new IntegrationRunRepository(txStore, this.log)

        await txMemberAttributeSettingsRepo.createPredefined(integrationService.memberAttributes)

        // delete the attributes cache
        const cache = new RedisCache('memberAttributes', this.redisClient, this.log)
        await cache.delete('default')

        await txRunRepo.updateIntegrationSettings(runId, {
          updateMemberAttributes: false,
        })
      })
    }

    const cache = new RedisCache(`int-${runInfo.integrationType}`, this.redisClient, this.log)

    const nangoConfig = NANGO_CONFIG()
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const nangoId = (runInfo.integrationSettings as any)?.nangoId
    const context: IGenerateStreamsContext = {
      onboarding: runInfo.onboarding,
      serviceSettings: {
        nangoUrl: nangoConfig.url,
        nangoSecretKey: nangoConfig.secretKey,
        nangoId: nangoId,
      },
      platformSettings: PLATFORM_CONFIG(runInfo.integrationType),
      integration: {
        id: runInfo.integrationId,
        identifier: runInfo.integrationIdentifier,
        platform: runInfo.integrationType as PlatformType,
        status: runInfo.integrationState,
        settings: runInfo.integrationSettings,
        token: runInfo.integrationToken,
        refreshToken: runInfo.integrationRefreshToken,
      },

      // this is for controling manual one off runs
      isManualRun,
      manualSettings,

      // this is mainly for github when we add new repos
      additionalInfo,

      log: this.log,
      cache,

      abortRunWithError: async (message: string, metadata?: unknown, error?: Error) => {
        this.log.error({ message }, 'Aborting run with error!')
        await this.triggerRunError(runId, 'run-abort', message, metadata, error)
      },

      publishStream: async (identifier: string, data?: unknown) => {
        await this.publishStream(
          runInfo.integrationType,
          runId,
          identifier,
          runInfo.onboarding,
          data,
        )
      },

      updateIntegrationSettings: async (settings: unknown) => {
        await this.updateIntegrationSettings(runId, settings)
      },

      updateIntegrationToken: async (token: string) => {
        await this.updateIntegrationToken(runId, token)
      },

      updateIntegrationRefreshToken: async (refreshToken: string) => {
        await this.updateIntegrationRefreshToken(runId, refreshToken)
      },
    }

    this.log.debug('Marking run as in progress!')
    await this.repo.markRunInProgress(runId)

    this.log.info('Generating streams!')
    try {
      await integrationService.generateStreams(context)
      this.log.info('Finished generating streams!')
    } catch (err) {
      this.log.error({ err }, 'Error while generating streams!')
      await this.triggerRunError(
        runId,
        'run-gen-streams',
        'Error while generating streams!',
        undefined,
        err,
      )
    }
  }

  private async updateIntegrationSettings(runId: string, settings: unknown): Promise<void> {
    try {
      this.log.debug('Updating integration settings!')
      await this.repo.updateIntegrationSettings(runId, settings)
    } catch (err) {
      await this.triggerRunError(
        runId,
        'run-update-settings',
        'Error while updating settings!',
        undefined,
        err,
      )
      throw err
    }
  }

  private async updateIntegrationToken(runId: string, token: string): Promise<void> {
    try {
      this.log.debug('Updating integration token!')
      await this.repo.updateIntegrationToken(runId, token)
    } catch (err) {
      await this.triggerRunError(
        runId,
        'run-update-token',
        'Error while updating token!',
        undefined,
        err,
      )
      throw err
    }
  }

  private async updateIntegrationRefreshToken(runId: string, refreshToken: string): Promise<void> {
    try {
      this.log.debug('Updating integration refresh token!')
      await this.repo.updateIntegrationRefreshToken(runId, refreshToken)
    } catch (err) {
      await this.triggerRunError(
        runId,
        'run-update-refresh-token',
        'Error while updating refresh token!',
        undefined,
        err,
      )
      throw err
    }
  }

  private async publishStream(
    platform: string,
    runId: string,
    identifier: string,
    onboarding: boolean,
    data?: unknown,
  ): Promise<void> {
    try {
      this.log.debug('Publishing new root stream!')
      const streamId = await this.repo.publishStream(runId, identifier, data)
      await this.streamWorkerEmitter.triggerStreamProcessing(platform, streamId, onboarding)
    } catch (err) {
      await this.triggerRunError(
        runId,
        'run-publish-root-stream',
        'Error while publishing root stream!',
        undefined,
        err,
      )
      throw err
    }
  }

  private async triggerRunError(
    runId: string,
    location: string,
    message: string,
    metadata?: unknown,
    error?: Error,
  ): Promise<void> {
    await this.repo.markRunError(runId, {
      location,
      message,
      metadata,
      errorMessage: error?.message,
      errorStack: error?.stack,
      errorString: error ? JSON.stringify(error) : undefined,
    })
  }
}
