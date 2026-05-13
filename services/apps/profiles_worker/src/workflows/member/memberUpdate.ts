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

const { updateMemberAffiliations, syncOrganization, syncMember } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '60 minutes',
})

export const refreshAffiliationsSignal = defineSignal<[MemberUpdateInput]>('refreshAffiliations')

/**
 * Per-member workflow that serializes async member operations (affiliations, sync, etc).
 * Concurrent signals are coalesced into one follow-up pass instead of racing.
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
    // signalWithStart starts with no args, so wait for the first signal to arrive
    const received = await condition(() => queued !== null, '5 minutes')
    if (!received) return
  }

  while (queued) {
    const pending = queued
    // Clear before awaiting so new signals accumulate into the next pass
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
