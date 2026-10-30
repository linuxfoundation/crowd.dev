import { LlmService, inferCountries } from '@crowd/common_services'
import {
  ICountryScanCandidate,
  ICountryScanCursor,
  ICountryScanWindow,
  QueryExecutor,
  SystemSettingKey,
  findMembersWithoutCountry,
  findOrganizationsWithoutCountry,
  getSystemSettingValue,
  pgpQx,
  saveMemberCountries,
  saveOrganizationCountries,
  setSystemSettingValue,
} from '@crowd/data-access-layer'

import { svc } from '../main'

const BATCH_SIZE = 500
// Data sink keeps an org's updatedAt for up to 30 minutes (updateOrganization throttle) and
// rows commit after their updatedAt, so the scan stops this far behind now to not skip them.
const SCAN_LAG_MS = 35 * 60 * 1000

function createLlmService(qx: QueryExecutor): LlmService {
  return new LlmService(
    qx,
    {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    svc.log,
  )
}

function nextCursor(candidates: ICountryScanCandidate[]): ICountryScanCursor | null {
  if (candidates.length < BATCH_SIZE) {
    return null
  }

  const { id, updatedAt } = candidates[candidates.length - 1]
  return { id, updatedAt }
}

export async function getCountryScanWindow(
  key: SystemSettingKey,
): Promise<{ since: string; until: string }> {
  const qx = pgpQx(svc.postgres.reader.connection())
  const lastSynced = await getSystemSettingValue(qx, key)
  const until = new Date(Date.now() - SCAN_LAG_MS).toISOString()

  return { since: lastSynced?.timestamp ?? until, until }
}

export async function saveCountryScanSyncedAt(
  key: SystemSettingKey,
  timestamp: string,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  await setSystemSettingValue(qx, key, { timestamp })
}

export async function processMemberCountries(
  window: ICountryScanWindow,
): Promise<ICountryScanCursor | null> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const members = await findMembersWithoutCountry(qx, window, BATCH_SIZE)

  const updates = await inferCountries(qx, createLlmService(qx), members, { withHints: true })
  await saveMemberCountries(qx, updates)

  return nextCursor(members)
}

export async function processOrganizationCountries(
  window: ICountryScanWindow,
): Promise<ICountryScanCursor | null> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const organizations = await findOrganizationsWithoutCountry(qx, window, BATCH_SIZE)

  const updates = await inferCountries(qx, createLlmService(qx), organizations, {
    withHints: false,
  })
  await saveOrganizationCountries(qx, updates)

  return nextCursor(organizations)
}
