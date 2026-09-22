/**
 * Entry point: Start Temporal worker + PCC project consumer loop.
 */
import { getServiceChildLogger } from '@crowd/logging'

import { createPccProjectConsumer } from './consumer/pccProjectConsumer'
import { svc } from './main'
import { schedulePccS3Cleanup, schedulePccS3Export } from './schedules'

const log = getServiceChildLogger('main')

const DRY_RUN = process.env.PCC_DRY_RUN === 'true'

setImmediate(async () => {
  try {
    // Create the consumer (and its DB connection) before svc.init() so we can
    // register the shutdown handler first. Node fires SIGINT/SIGTERM listeners
    // in registration order; registering ours before the archetype's lets the
    // consumer drain before shared infra (Postgres, Temporal) is torn down.
    const consumer = await createPccProjectConsumer(DRY_RUN)

    const HARD_TIMEOUT_MS = 2 * 60 * 60 * 1000
    const shutdown = () => {
      log.info('Shutdown signal received, stopping consumer...')
      consumer.stop()

      setTimeout(() => {
        log.warn('Graceful shutdown timed out after 2 hours, forcing exit')
        process.exit(1)
      }, HARD_TIMEOUT_MS).unref()
    }

    process.on('SIGINT', shutdown)
    process.on('SIGTERM', shutdown)

    await svc.init()

    await schedulePccS3Export()
    await schedulePccS3Cleanup()

    consumer.start().catch((err) => {
      log.error({ err }, 'Consumer loop crashed')
      process.exit(1)
    })

    await svc.start()
  } catch (err) {
    log.error({ err }, 'PCC worker startup failed')
    process.exit(1)
  }
})
