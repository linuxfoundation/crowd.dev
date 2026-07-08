import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'
import { getPackagesDb } from '../db'

import { IngestSingleResult, ingestSecurityContactsForPurl } from './ingestSingle'
import { BatchResult, processBatch } from './processBatch'

const log = getServiceChildLogger('security-contacts-activity')

export async function processSecurityContactsBatch(): Promise<BatchResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()

  const result = await processBatch(qx, config)
  log.info({ ...result }, 'Security contacts batch activity complete')
  return result
}

export async function ingestSecurityContactsForPurlActivity(
  purl: string,
): Promise<IngestSingleResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()

  const result = await ingestSecurityContactsForPurl(qx, config, purl)
  log.info({ purl, ...result }, 'On-demand security contacts ingest activity complete')
  return result
}
