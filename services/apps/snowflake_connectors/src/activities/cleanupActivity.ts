import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { MetadataStore } from '../core/metadataStore'
import { S3Service } from '../core/s3Service'

const log = getServiceChildLogger('cleanupActivity')

export async function executeCleanup(intervalHours = 24): Promise<void> {
  const db = await getDbConnection(WRITE_DB_CONFIG())
  const metadataStore = new MetadataStore(db)
  const s3Service = new S3Service()

  const jobs = await metadataStore.getCleanableJobS3Paths(intervalHours)
  log.info({ jobCount: jobs.length, intervalHours }, 'Found cleanable jobs')

  for (const job of jobs) {
    try {
      await s3Service.deleteFile(job.s3Path)
      await metadataStore.markCleaned(job.id)
      log.info({ jobId: job.id, s3Path: job.s3Path }, 'Cleaned job')
    } catch (err) {
      log.error({ jobId: job.id, s3Path: job.s3Path, err }, 'Failed to clean job, skipping')
      sendSlackNotification(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.ERROR_REPORTER,
        'Snowflake S3 Cleanup Failed',
        `Failed to clean job \`${job.id}\` at \`${job.s3Path}\`.\n\n*Error:* ${err instanceof Error ? err.message : err}`,
      )
    }
  }
}
