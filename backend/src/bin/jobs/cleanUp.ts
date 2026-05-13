import { getServiceChildLogger } from '@crowd/logging'

import IncomingWebhookRepository from '../../database/repositories/incomingWebhookRepository'
import IntegrationRunRepository from '../../database/repositories/integrationRunRepository'
import SequelizeRepository from '../../database/repositories/sequelizeRepository'
import { CrowdJob } from '../../types/jobTypes'

const MAX_MONTHS_TO_KEEP = 3

const log = getServiceChildLogger('cleanUp')

export const cleanUpOldRuns = async () => {
  const dbOptions = await SequelizeRepository.getDefaultIRepositoryOptions()
  const repo = new IntegrationRunRepository(dbOptions)

  log.info(
    `Cleaning up processed integration runs that are older than ${MAX_MONTHS_TO_KEEP} months!`,
  )
  await repo.cleanupOldRuns(MAX_MONTHS_TO_KEEP)
}

export const cleanUpOrphanedWebhooks = async () => {
  const dbOptions = await SequelizeRepository.getDefaultIRepositoryOptions()
  const repo = new IncomingWebhookRepository(dbOptions)

  log.info(
    `Cleaning up orphaned incoming webhooks that doesn't belong to any integrations anymore!`,
  )
  await repo.cleanUpOrphanedWebhooks()
}

export const cleanUpOldWebhooks = async () => {
  const dbOptions = await SequelizeRepository.getDefaultIRepositoryOptions()
  const repo = new IncomingWebhookRepository(dbOptions)

  log.info(
    `Cleaning up processed incoming webhooks that are older than ${MAX_MONTHS_TO_KEEP} months!`,
  )
  await repo.cleanUpOldWebhooks(MAX_MONTHS_TO_KEEP)
}

const job: CrowdJob = {
  name: 'Clean up old data',
  // run once every week on Sunday at 1AM
  cronTime: '0 1 * * 0',
  onTrigger: async () => {
    await Promise.all([cleanUpOldRuns(), cleanUpOldWebhooks(), cleanUpOrphanedWebhooks()])
  },
}

export default job
