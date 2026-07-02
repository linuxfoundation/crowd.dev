import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'
import { getPackagesDb } from '../db'

import { BatchResult, processBatch } from './processBatch'

const log = getServiceChildLogger('security-contacts-activity')

export async function processSecurityContactsBatch(): Promise<BatchResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()

  const result = await processBatch(qx, config)
  log.info({ ...result }, 'Security contacts batch activity complete')
  return result
}
