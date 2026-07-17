import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'
import { getCdpDb, getPackagesDb } from '../db'

import { IngestSingleResult, ingestSecurityContactsForPurl } from './ingestSingle'
import { BatchResult, processBatch } from './processBatch'

const log = getServiceChildLogger('security-contacts-activity')

export async function processSecurityContactsBatch(): Promise<BatchResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()
  const cdpQx = await getCdpDb()

  const result = await processBatch(qx, cdpQx, config)
  log.info({ ...result }, 'Security contacts batch activity complete')
  return result
}

export async function ingestSecurityContactsForPurlActivity(
  purl: string,
): Promise<IngestSingleResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()
  const cdpQx = await getCdpDb()

  const result = await ingestSecurityContactsForPurl(qx, cdpQx, config, purl)
  log.info({ purl, ...result }, 'On-demand security contacts ingest activity complete')
  return result
}
