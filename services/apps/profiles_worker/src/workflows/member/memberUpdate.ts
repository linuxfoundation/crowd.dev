import {
  condition,
  continueAsNew,
  defineSignal,
  proxyActivities,
  setHandler,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../../activities'
import { MemberUpdateInput } from '../../types/member'

// Configure timeouts and retry policies to update a member in the database.
const { updateMemberAffiliations, syncOrganization, syncMember } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '60 minutes',
})

export const refreshAffiliationsSignal = defineSignal<[MemberUpdateInput]>('refreshAffiliations')

/*
memberUpdate is a Temporal workflow that:
  - [Signal]: accepts 'refreshAffiliations' signals to queue affiliation refresh requests
  - [Activity]: Refresh all affiliations for a given member in the database.
  - [Activity]: Sync member and memberOrganizations to OpenSearch if specified.

Signals are coalesced: if N requests arrive while a refresh is in progress,
they are merged into one follow-up pass. This eliminates the TERMINATE_IF_RUNNING
race where concurrent callers killed mid-flight refreshes.
*/
export async function memberUpdate(input?: MemberUpdateInput): Promise<void> {
  let queued: MemberUpdateInput | null = input ?? null

  setHandler(refreshAffiliationsSignal, (next: MemberUpdateInput) => {
    if (!queued) {
      queued = next
    } else {
      queued = {
        member: queued.member,
        memberOrganizationIds: [
          ...new Set([
            ...(queued.memberOrganizationIds || []),
            ...(next.memberOrganizationIds || []),
          ]),
        ],
        syncToOpensearch: queued.syncToOpensearch || next.syncToOpensearch,
      }
    }
  })

  if (!queued) {
    const received = await condition(() => queued !== null, '5 minutes')
    if (!received) return
  }

  while (queued) {
    const pending = queued
    queued = null

    const memberId = pending.member.id
    await updateMemberAffiliations(memberId)
    if (pending.syncToOpensearch) {
      await syncMember(memberId)
      for (const orgId of pending.memberOrganizationIds || []) {
        await syncOrganization(orgId)
      }
    }

    if (workflowInfo().continueAsNewSuggested && queued) {
      await continueAsNew<typeof memberUpdate>(queued)
      return
    }
  }
}
