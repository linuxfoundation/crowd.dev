import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { bucket } from '../config'

const log = getServiceChildLogger('cleanupGcs')

const GCS_PREFIX = 'osspckgs/'
const MAX_AGE_MS = 24 * 60 * 60 * 1000

export async function cleanupGcs(): Promise<{ deletedObjects: number; markedJobs: number }> {
  const cutoff = new Date(Date.now() - MAX_AGE_MS)

  // Delete GCS objects under osspckgs/ prefix older than 24h
  const [files] = await bucket.getFiles({ prefix: GCS_PREFIX })
  let deletedObjects = 0
  for (const file of files) {
    const created = new Date(file.metadata.timeCreated as string)
    if (created < cutoff) {
      await file.delete()
      deletedObjects++
    }
  }

  // Mark cleaned_at on finished jobs older than 24h
  const qx = await getPackagesDb()
  const markedJobs = await qx.result(`
    UPDATE osspckgs_ingest_jobs
    SET cleaned_at = NOW()
    WHERE status = 'done'
      AND cleaned_at IS NULL
      AND finished_at < $(cutoff)
  `, { cutoff })

  log.info({ deletedObjects, markedJobs }, 'GCS cleanup complete')
  return { deletedObjects, markedJobs }
}
