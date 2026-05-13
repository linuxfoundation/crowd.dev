import { proxyActivities } from '@temporalio/workflow'

import { MemberEnrichmentMaterializedView } from '@crowd/types'

import * as activities from '../activities'

const { refreshMemberEnrichmentMaterializedView } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
})

export async function refreshMemberEnrichmentMaterializedViews(): Promise<void> {
  for (const mv of Object.values(MemberEnrichmentMaterializedView)) {
    await refreshMemberEnrichmentMaterializedView(mv)
  }
}
