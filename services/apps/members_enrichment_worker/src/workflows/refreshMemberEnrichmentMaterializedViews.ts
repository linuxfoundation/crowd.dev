import { proxyActivities } from '@temporalio/workflow'

import { MemberEnrichmentMaterializedView } from '@crowd/types'

import * as activities from '../activities'

const { refreshMemberEnrichmentMaterializedView } = proxyActivities<typeof activities>({
  startToCloseTimeout: '45 minutes',
  retry: {
    initialInterval: '15 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
  },
})

export async function refreshMemberEnrichmentMaterializedViews(): Promise<void> {
  for (const mv of Object.values(MemberEnrichmentMaterializedView)) {
    await refreshMemberEnrichmentMaterializedView(mv)
  }
}
