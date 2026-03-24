import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  continueAsNew,
  proxyActivities,
  startChild,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'
import { IBlockOrganizationAffiliationArgs } from '../types'
import { chunkArray } from '../utils/common'

import { recalculateMemberAffiliations } from './recalculate-member-affiliations'

const {
  setOrganizationAffiliationPolicyIfNotBlocked,
  fetchProjectMemberOrganizationsToBlock,
  blockMemberOrganizationAffiliation,
  markMemberForAffiliationRecalc,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
})

export async function blockProjectOrganizationAffiliations(
  args: IBlockOrganizationAffiliationArgs,
): Promise<void> {
  const info = workflowInfo()

  const MEMBERS_PER_RUN = args.batchSize ?? 1000
  const afterId = args.afterId ?? undefined

  const memberOrganizations = await fetchProjectMemberOrganizationsToBlock(MEMBERS_PER_RUN, afterId)

  if (memberOrganizations?.length === 0) {
    console.log('No more organization members to block!')

    // Once we finish blocking affiliations, we need to recalculate member affiliations.
    // It's done separately so we can handle it better and keep it under control.
    await startChild(recalculateMemberAffiliations, {
      workflowId: `recalculateMemberAffiliations/${info.workflowId}`,
      cancellationType: ChildWorkflowCancellationType.ABANDON,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      retry: {
        backoffCoefficient: 2,
        initialInterval: 2 * 1000,
        maximumInterval: 30 * 1000,
      },
      args: [
        {
          batchSize: 500,
        },
      ],
    })

    return
  }

  const CONCURRENCY = 10
  const BATCH_SIZE = 50

  // Step 1: Set organization-level affiliation policy
  // Get unique organizationIds and set the policy for each organization
  const uniqueOrganizationIds = Array.from(
    new Set(memberOrganizations.map((mo) => mo.organizationId)),
  )

  // Update organization affiliation policies in parallel batches
  // Combined check and update reduces Temporal activity costs
  for (const batch of chunkArray(uniqueOrganizationIds, CONCURRENCY)) {
    await Promise.all(batch.map((orgId) => setOrganizationAffiliationPolicyIfNotBlocked(orgId)))
  }

  // Step 2: Block member-level affiliations in batches
  // Transform memberOrganizations into override payloads and chunk for bulk upsert
  const batches = chunkArray(memberOrganizations, BATCH_SIZE).map((chunk) =>
    chunk.map((mo) => ({
      memberId: mo.memberId,
      memberOrganizationId: mo.id,
      allowAffiliation: false,
    })),
  )

  for (let i = 0; i < batches.length; i += CONCURRENCY) {
    const slice = batches.slice(i, i + CONCURRENCY)
    await Promise.all(slice.map((batch) => blockMemberOrganizationAffiliation(batch)))
  }

  // Step 3: Deduplicate memberIds and queue for affiliation recalculation
  const uniqueMemberIds = Array.from(new Set(memberOrganizations.map((mo) => mo.memberId)))

  // A member can have multiple memberOrganizations, but we only need to recalc affiliation once per member
  // Redis set ensures no duplicates even if same memberId appears across multiple batches
  await markMemberForAffiliationRecalc(uniqueMemberIds)

  if (args.testRun) {
    console.log('[DEBUG] Processed memberIds:', uniqueMemberIds)
    console.log('[DEBUG] Processed organizationIds:', uniqueOrganizationIds)
    console.log('Test run completed - stopping after first batch!')
    return
  }

  // Step 4: Continue pagination using keyset (last processed memberOrganization.id)
  const lastProcessedId = memberOrganizations[memberOrganizations.length - 1]?.id

  await continueAsNew<typeof blockProjectOrganizationAffiliations>({
    ...args,
    afterId: lastProcessedId,
  })
}
