import { fetchOrganizationMemberIds, pgpQx } from '@crowd/data-access-layer'
import { SearchSyncApiClient } from '@crowd/opensearch'

import { svc } from '../../main'

export async function findMembersInOrganization(
  organizationId: string,
  limit: number,
  afterMemberId?: string,
): Promise<string[]> {
  return fetchOrganizationMemberIds(
    pgpQx(svc.postgres.reader.connection()),
    organizationId,
    limit,
    afterMemberId,
  )
}

export async function syncOrganization(organizationId: string): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })

  await syncApi.triggerOrganizationSync(organizationId, undefined)
}
