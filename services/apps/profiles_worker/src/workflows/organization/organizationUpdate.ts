import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import * as activities from '../../activities'
import { IOrganizationProfileSyncInput } from '../../types/organization'

// Configure timeouts and retry policies to update a member in the database.
const { updateMemberAffiliations, syncOrganization, findMembersInOrganization } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '5 minutes',
})

/*
organizationUpdate can do the following:
  - Update all affiliations of members in a given organization in the database, if recalculateAffiliations is true.
  - Sync organization to OpenSearch, if syncOptions.doSync is true.
  - Also sync organization aggregates to postgres (organizationSegmentsAgg table), if syncOptions.doSync is true AND syncOptions.withAggs is true.
*/
export async function organizationUpdate(input: IOrganizationProfileSyncInput): Promise<void> {
  // End early if recalculateAffiliations is false, only do syncing if necessary.
  if (!input.recalculateAffiliations) {
    if (input.syncOptions?.doSync) {
      await syncOrganization(input.organization.id)
    }

    return
  }

  const ORGANIZATION_MEMBER_AFFILIATIONS_UPDATED_PER_RUN = 500

  const memberIds = await findMembersInOrganization(
    input.organization.id,
    ORGANIZATION_MEMBER_AFFILIATIONS_UPDATED_PER_RUN,
    input.afterMemberId,
  )

  if (memberIds.length === 0) {
    if (input.syncOptions?.doSync) {
      // sync organization
      await syncOrganization(input.organization.id)
    }
    return
  }

  for (const memberId of memberIds) {
    await updateMemberAffiliations(memberId)
  }

  await continueAsNew<typeof organizationUpdate>({
    ...input,
    afterMemberId: memberIds[memberIds.length - 1],
  })
}
