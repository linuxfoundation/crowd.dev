import { signalMemberUpdate } from '@crowd/common_services'
import { refreshMemberOrganizationAffiliations } from '@crowd/data-access-layer/src/member-organization-affiliation'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SearchSyncApiClient } from '@crowd/opensearch'

import { svc } from '../../main'

export async function updateMemberAffiliations(memberId: string): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  await refreshMemberOrganizationAffiliations(qx, memberId)
}

export async function triggerMemberAffiliationsRefresh(
  memberId: string,
  memberOrganizationIds: string[] = [],
  syncToOpensearch = false,
): Promise<void> {
  await signalMemberUpdate(svc.temporal, memberId, {
    memberOrganizationIds,
    syncToOpensearch,
  })
}

export async function syncMember(memberId: string): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })

  await syncApi.triggerMemberSync(memberId)
}
