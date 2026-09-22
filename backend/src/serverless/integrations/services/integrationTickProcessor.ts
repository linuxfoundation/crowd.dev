import IntegrationRepository from '@/database/repositories/integrationRepository'
import { processPaginated, singleOrDefault } from '@crowd/common'
import {
  DataSinkWorkerEmitter,
  IntegrationRunWorkerEmitter,
  IntegrationStreamWorkerEmitter,
} from '@crowd/common_services'
import { INTEGRATION_SERVICES } from '@crowd/integrations'
import { LoggerBase, getChildLogger } from '@crowd/logging'
import { IntegrationType } from '@crowd/types'

import IntegrationRunRepository from '../../../database/repositories/integrationRunRepository'
import { IServiceOptions } from '../../../services/IServiceOptions'
import {
  getDataSinkWorkerEmitter,
  getIntegrationRunWorkerEmitter,
  getIntegrationStreamWorkerEmitter,
} from '../../utils/queueService'

export class IntegrationTickProcessor extends LoggerBase {
  private tickTrackingMap: Map<IntegrationType, number> = new Map()

  private emittersInitialized = false

  private intRunWorkerEmitter: IntegrationRunWorkerEmitter

  private intStreamWorkerEmitter: IntegrationStreamWorkerEmitter

  private dataSinkWorkerEmitter: DataSinkWorkerEmitter

  constructor(
    options: IServiceOptions,
    private readonly integrationRunRepository: IntegrationRunRepository,
  ) {
    super(options.log)

    for (const intService of INTEGRATION_SERVICES) {
      this.tickTrackingMap[intService.type] = 0
    }
  }

  async initEmitters() {
    if (!this.emittersInitialized) {
      this.intRunWorkerEmitter = await getIntegrationRunWorkerEmitter()
      this.intStreamWorkerEmitter = await getIntegrationStreamWorkerEmitter()
      this.dataSinkWorkerEmitter = await getDataSinkWorkerEmitter()

      this.emittersInitialized = true
    }
  }

  async processTick(): Promise<void> {
    await this.processCheckTick()
    await this.processDelayedTick()
  }

  private async processCheckTick() {
    this.log.trace('Processing integration processor tick!')

    const tickers: IIntTicker[] = INTEGRATION_SERVICES.map((i) => ({
      type: i.type,
      ticksBetweenChecks: i.checkEvery || -1,
    }))

    const promises: Promise<void>[] = []

    for (const intService of tickers) {
      let trigger = false

      if (intService.ticksBetweenChecks < 0) {
        this.log.debug({ type: intService.type }, 'Integration is set to never be triggered.')
      } else if (intService.ticksBetweenChecks === 0) {
        this.log.warn({ type: intService.type }, 'Integration is set to be always triggered.')
        trigger = true
      } else {
        this.tickTrackingMap[intService.type]++

        if (this.tickTrackingMap[intService.type] === intService.ticksBetweenChecks) {
          this.log.info(
            { type: intService.type, tickCount: intService.ticksBetweenChecks },
            'Integration is being triggered since it reached its target tick count!',
          )
          trigger = true
          this.tickTrackingMap[intService.type] = 0
        }
      }

      if (trigger) {
        this.log.info({ type: intService.type }, 'Triggering integration check!')
        promises.push(
          this.processCheck(intService.type as IntegrationType).catch((err) => {
            this.log.error(err, 'Error while processing integration check!')
          }),
        )
      }
    }

    if (promises.length > 0) {
      await Promise.all(promises)
    }
  }

  async fixIntegrationRuns(integrationId: string, logger) {
    await this.integrationRunRepository.cleanupOrphanedIntegrationRuns(integrationId)

    const stuckRuns = await this.integrationRunRepository.findStuckIntegrationRuns(integrationId)
    logger.info(
      `${stuckRuns.length} stuck integration runs found for integrations ${integrationId}`,
    )
    await this.initEmitters()
    for (const run of stuckRuns) {
      logger.info(`Retrying streams for stuck run: ${run.id}`)
      await this.intStreamWorkerEmitter.continueProcessingRunStreams(
        run.onboarding,
        undefined,
        run.id,
      )
    }
  }

  async processCheck(type: IntegrationType) {
    const logger = getChildLogger('processCheck', this.log, { IntegrationType: type })
    logger.trace('Processing integration check!')

    const newIntService = singleOrDefault(INTEGRATION_SERVICES, (i) => i.type === type)

    if (!newIntService) {
      throw new Error(`No integration service found for type ${type}!`)
    }

    const emitter = await getIntegrationRunWorkerEmitter()

    await processPaginated(
      async (page) => IntegrationRepository.findAllActive(type, page, 10),
      async (integrations) => {
        logger.debug(
          { integrationIds: integrations.map((i) => i.id) },
          'Found new integrations to check!',
        )
        for (const integration of integrations as any[]) {
          await this.fixIntegrationRuns(integration.id, logger)
          const existingRun =
            await this.integrationRunRepository.findLastProcessingRunInNewFramework(integration.id)
          if (!existingRun) {
            const CHUNKS = 3 // Define the number of chunks
            const DELAY_BETWEEN_CHUNKS = 30 * 60 * 1000 // Define the delay between chunks in milliseconds
            const rand = Math.random() * CHUNKS
            const chunkIndex = Math.min(Math.floor(rand), CHUNKS - 1)
            const delay = chunkIndex * DELAY_BETWEEN_CHUNKS

            // Divide integrations into chunks for Discord
            if (newIntService.type === IntegrationType.DISCORD) {
              setTimeout(async () => {
                logger.info(
                  { integrationId: integration.id },
                  `Triggering new delayed integration check for Discord in ${
                    delay / 60 / 1000
                  } minutes!`,
                )
                await emitter.triggerIntegrationRun(integration.platform, integration.id, false)
              }, delay)
            } else {
              logger.info({ integrationId: integration.id }, 'Triggering new integration check!')
              await emitter.triggerIntegrationRun(integration.platform, integration.id, false)
            }
          } else {
            logger.info({ integrationId: integration.id }, 'Existing run found, skipping!')
          }
        }
      },
    )
  }

  private async processDelayedTick() {
    await this.initEmitters()
    await this.intRunWorkerEmitter.checkRuns()
    await this.intStreamWorkerEmitter.checkStreams()
    await this.dataSinkWorkerEmitter.checkResults()
  }
}

interface IIntTicker {
  type: string
  ticksBetweenChecks: number
}
