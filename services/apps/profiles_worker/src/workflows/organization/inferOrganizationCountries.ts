import { proxyActivities } from '@temporalio/workflow'

import type { ICountryScanCursor } from '@crowd/data-access-layer'

import * as activities from '../../activities'

const { getCountryScanWindow, saveCountryScanSyncedAt, processOrganizationCountries } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: '10 minutes',
    retry: { maximumAttempts: 3 },
  })

const LAST_SYNCED_AT_KEY = 'organizationCountryInferenceLastSyncedAt'

export async function inferOrganizationCountries(): Promise<void> {
  const { since, until } = await getCountryScanWindow(LAST_SYNCED_AT_KEY)

  let after: ICountryScanCursor | null = null
  do {
    after = await processOrganizationCountries({ since, until, after })
  } while (after)

  await saveCountryScanSyncedAt(LAST_SYNCED_AT_KEY, until)
}
