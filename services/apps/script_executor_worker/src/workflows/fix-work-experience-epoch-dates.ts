import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  continueAsNew,
  proxyActivities,
  startChild,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'
import { IScriptBatchTestArgs } from '../types'
import { chunkArray } from '../utils/common'

import { recalculateMemberAffiliations } from './recalculate-member-affiliations'

const {
  findMemberWorkExperienceWithEpochDates,
  updateMemberWorkExperience,
  markMemberForAffiliationRecalc,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
})

export async function fixWorkExperienceEpochDates(args: IScriptBatchTestArgs): Promise<void> {
  const info = workflowInfo()
  const WORK_EXPERIENCES_PER_RUN = args.batchSize ?? 1000

  const workExperiences = await findMemberWorkExperienceWithEpochDates(WORK_EXPERIENCES_PER_RUN)

  if (workExperiences?.length === 0) {
    console.log('No more work experiences to fix, triggering recalculation of member affiliations!')

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

  const isEpoch = (v?: string | Date | null) => {
    if (!v) return false
    const d = v instanceof Date ? v : new Date(v)
    return d.getTime() === 0
  }

  for (const chunk of chunkArray(workExperiences, 10)) {
    await Promise.all(
      chunk.map((we) => {
        // prepare original object
        const original = {
          id: we.id,
          orgId: we.organizationId,
          jobTitle: we.title,
          dateStart: we.dateStart as string,
          dateEnd: we.dateEnd as string,
          source: we.source,
        }

        // prepare toUpdate object
        const toUpdate = {
          ...(isEpoch(original.dateStart) && { dateStart: null }),
          ...(isEpoch(original.dateEnd) && { dateEnd: null }),
        }

        if (args.testRun) {
          console.log(`Updating work experience for member ${we.memberId}`)
          console.log(`Original: ${JSON.stringify(original)}`)
        }

        return updateMemberWorkExperience(we.memberId, original, toUpdate)
      }),
    )
  }

  // deduplicate memberIds and queue for affiliation recalculation
  const uniqueMemberIds = Array.from(new Set(workExperiences.map((we) => we.memberId)))

  await markMemberForAffiliationRecalc(uniqueMemberIds)

  if (args.testRun) {
    console.log('Test run completed - stopping after first batch!')
    return
  }

  await continueAsNew<typeof fixWorkExperienceEpochDates>(args)
}
