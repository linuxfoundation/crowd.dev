import { singleOrDefault } from '@crowd/common'
import { DataSinkWorkerEmitter, IntegrationStreamWorkerEmitter } from '@crowd/common_services'
import { DbStore } from '@crowd/data-access-layer/src/database'
import { IStreamData } from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.data'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { INTEGRATION_SERVICES, IProcessDataContext } from '@crowd/integrations'
import { Logger, LoggerBase } from '@crowd/logging'
import { RedisCache, RedisClient } from '@crowd/redis'
import { IActivityData, IntegrationResultType, PlatformType } from '@crowd/types'

import { PLATFORM_CONFIG } from '../conf'

export default class IntegrationDataService extends LoggerBase {
  private repo: IntegrationStreamRepository

  constructor(
    private readonly redisClient: RedisClient,
    private readonly streamWorkerEmitter: IntegrationStreamWorkerEmitter,
    private readonly dataSinkWorkerEmitter: DataSinkWorkerEmitter,
    store: DbStore,
    parentLog: Logger,
  ) {
    super(parentLog)

    this.repo = new IntegrationStreamRepository(store, this.log)
  }

  public async processData(data: unknown, stream: IStreamData): Promise<void> {
    const integrationService = singleOrDefault(
      INTEGRATION_SERVICES,
      (i) => i.type === stream.integrationType,
    )

    if (!integrationService) {
      this.log.error({ type: stream.integrationType }, 'Could not find integration service!')
      throw new Error('Could not find integration service to process API data!')
    }

    const cache = new RedisCache(`int-${stream.integrationType}`, this.redisClient, this.log)

    const context: IProcessDataContext = {
      onboarding: stream.onboarding !== null ? stream.onboarding : undefined,
      platformSettings: PLATFORM_CONFIG(stream.integrationType),
      integration: {
        id: stream.integrationId,
        identifier: stream.integrationIdentifier,
        platform: stream.integrationType as PlatformType,
        status: stream.integrationState,
        settings: stream.integrationSettings,
        token: stream.integrationToken,
        refreshToken: stream.integrationRefreshToken,
      },

      data,

      log: this.log,
      cache,

      publishActivity: async (activity) => {
        await this.publishActivity(
          stream.id,
          stream.integrationType,
          stream.onboarding === null ? true : stream.onboarding,
          activity,
        )
      },

      publishCustom: async (entity, type) => {
        await this.publishCustom(
          stream.id,
          stream.integrationType,
          stream.onboarding === null ? true : stream.onboarding,
          type,
          entity,
        )
      },

      publishStream: async (identifier, data) => {
        await this.publishStream(
          stream.integrationType,
          stream.id,
          identifier,
          stream.onboarding === null ? true : stream.onboarding,
          data,
          stream.runId,
          stream.webhookId,
        )
      },

      updateIntegrationSettings: async (settings) => {
        await this.updateIntegrationSettings(stream.id, settings)
      },

      updateIntegrationToken: async (token: string) => {
        await this.updateIntegrationToken(stream.id, token)
      },

      updateIntegrationRefreshToken: async (refreshToken: string) => {
        await this.updateIntegrationRefreshToken(stream.id, refreshToken)
      },

      abortWithError: async (message: string, metadata?: unknown, error?: Error) => {
        this.log.error(error, { message, metadata }, 'Aborting stream processing with error!')
        throw new Error(`Aborting API data processing with error! Message: ${message}`)
      },

      abortRunWithError: stream.runId
        ? async (message: string, metadata?: unknown, error?: Error) => {
            this.log.error({ message }, 'Aborting run with error from a data processor!')
            throw {
              runError: true,
              location: 'integration-data-processing',
              message,
              metadata,
              originalError: error,
            }
          }
        : undefined,
    }

    this.log.debug('Processing data!')
    try {
      await integrationService.processData(context)
      this.log.debug('Finished processing data!')
    } catch (err) {
      this.log.error(err, 'Error while processing stream!')
      throw err
    }
  }

  private async publishCustom(
    streamId: string,
    platform: string,
    onboarding: boolean,
    type: IntegrationResultType,
    entity: unknown,
  ): Promise<void> {
    this.log.debug(`Publishing entity with custom type!`)

    try {
      const resultId = await this.repo.publishResult(streamId, {
        type,
        data: entity,
      })
      await this.dataSinkWorkerEmitter.triggerResultProcessing(resultId, resultId, onboarding)
    } catch (err) {
      this.log.error(err, { platform }, 'Error while publishing custom result!')
      throw new Error('Error while publishing custom result!')
    }
  }

  private async publishActivity(
    streamId: string,
    platform: string,
    onboarding: boolean,
    activity: IActivityData,
  ): Promise<void> {
    try {
      this.log.debug('Publishing activity!')
      const resultId = await this.repo.publishResult(streamId, {
        type: IntegrationResultType.ACTIVITY,
        data: activity,
      })
      await this.dataSinkWorkerEmitter.triggerResultProcessing(
        resultId,
        activity.sourceId,
        onboarding,
      )
    } catch (err) {
      this.log.error(err, { platform }, 'Error while publishing activity!')
      throw new Error('Error while publishing activity!')
    }
  }

  private async updateIntegrationSettings(streamId: string, settings: unknown): Promise<void> {
    try {
      this.log.debug('Updating integration settings!')
      await this.repo.updateIntegrationSettings(streamId, settings)
    } catch (err) {
      this.log.error(err, { streamId }, 'Error while updating integration settings!')
      throw err
    }
  }

  private async updateIntegrationToken(streamId: string, token: string): Promise<void> {
    try {
      this.log.debug('Updating integration token!')
      await this.repo.updateIntegrationToken(streamId, token)
    } catch (err) {
      this.log.error(err, { streamId }, 'Error while updating integration token!')
      throw err
    }
  }

  private async updateIntegrationRefreshToken(
    streamId: string,
    refreshToken: string,
  ): Promise<void> {
    try {
      this.log.debug('Updating integration refresh token!')
      await this.repo.updateIntegrationRefreshToken(streamId, refreshToken)
    } catch (err) {
      this.log.error(err, { streamId }, 'Error while updating integration refresh token!')
      throw err
    }
  }

  private async publishStream(
    platform: string,
    parentId: string,
    identifier: string,
    onboarding: boolean,
    data?: unknown,
    runId?: string,
    webhookId?: string,
  ): Promise<void> {
    try {
      this.log.debug({ identifier }, 'Publishing new child stream!')
      if (!runId && !webhookId) {
        throw new Error('Need either runId or webhookId!')
      }

      const streamId = await this.repo.publishStream(parentId, identifier, data, runId, webhookId)
      if (streamId) {
        if (runId) {
          await this.streamWorkerEmitter.triggerStreamProcessing(platform, streamId, onboarding)
        } else if (webhookId) {
          await this.streamWorkerEmitter.triggerWebhookProcessing(platform, webhookId)
        } else {
          this.log.error(
            { platform, parentId, identifier, runId, webhookId },
            'Need either runId or webhookId!',
          )
          throw new Error('Need either runId or webhookId!')
        }
      } else {
        this.log.debug({ identifier }, 'Child stream already exists!')
      }
    } catch (err) {
      throw {
        runError: true,
        location: 'integration-data-processing-publish-stream',
        message: 'Error while publishing a child stream from data processor!',
        originalError: err,
      }
    }
  }
}
